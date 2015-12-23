/*
 *  Copyright 2015 esbtools Contributors and/or its affiliates.
 *
 *  This file is part of esbtools.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.esbtools.eventhandler.lightblue;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.model.DataError;
import com.redhat.lightblue.client.model.Error;
import com.redhat.lightblue.client.request.AbstractLightblueDataRequest;
import com.redhat.lightblue.client.request.DataBulkRequest;
import com.redhat.lightblue.client.response.LightblueBulkDataResponse;
import com.redhat.lightblue.client.response.LightblueDataResponse;
import com.redhat.lightblue.client.response.LightblueErrorResponse;
import com.redhat.lightblue.client.response.LightblueException;
import com.redhat.lightblue.client.response.LightblueResponse;

import org.esbtools.eventhandler.ResponsesHandler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BulkLightblueRequester implements LightblueRequester {
    private final LightblueClient lightblue;
    private final Map<LazyFuture, AbstractLightblueDataRequest[]> queuedRequests =
            Collections.synchronizedMap(new HashMap<>());

    public BulkLightblueRequester(LightblueClient lightblue) {
        this.lightblue = lightblue;
    }

    @Override
    public LightblueResponsePromise queueRequests(AbstractLightblueDataRequest... requests) {
        return new BulkResponsePromise(requests);
    }

    private void doQueuedRequestsAndPopulateResults() {
        Map<LazyFuture, AbstractLightblueDataRequest[]> batch;

        synchronized (queuedRequests) {
            batch = new HashMap<>(queuedRequests);
            queuedRequests.clear();
        }

        DataBulkRequest bulkRequest = new DataBulkRequest();

        for (AbstractLightblueDataRequest[] allRequestBatches : batch.values()) {
            for (AbstractLightblueDataRequest requestInBatch : allRequestBatches) {
                bulkRequest.add(requestInBatch);
            }
        }

        try {
            LightblueBulkDataResponse bulkResponse = lightblue.bulkData(bulkRequest);

            for (Entry<LazyFuture, AbstractLightblueDataRequest[]> lazyResultToRequests : batch.entrySet()) {
                LazyFuture lazyFuture = lazyResultToRequests.getKey();
                AbstractLightblueDataRequest[] requests = lazyResultToRequests.getValue();
                Map<AbstractLightblueDataRequest, LightblueDataResponse> responseMap = new HashMap<>(requests.length);
                List<Error> errors = new ArrayList<>();

                for (AbstractLightblueDataRequest request : requests) {
                    LightblueResponse response = bulkResponse.getResponse(request);

                    if (response instanceof LightblueErrorResponse) {
                        LightblueErrorResponse errorResponse = (LightblueErrorResponse) response;

                        for (DataError dataError : errorResponse.getDataErrors()) {
                            errors.addAll(dataError.getErrors());
                        }

                        Collections.addAll(errors, errorResponse.getLightblueErrors());
                    }

                    if (response instanceof LightblueDataResponse) {
                        responseMap.put(request, (LightblueDataResponse) response);
                    }
                }

                if (errors.isEmpty()) {
                    lazyFuture.complete(new BulkResponses(responseMap));
                } else {
                    lazyFuture.completeExceptionally(new BulkLightblueResponseException(errors));
                }
            }
        } catch (LightblueException e) {
            for (Entry<LazyFuture, AbstractLightblueDataRequest[]> lazyResultToRequests : batch.entrySet()) {
                LazyFuture lazyResult = lazyResultToRequests.getKey();
                lazyResult.completeExceptionally(e);
            }
        }
    }

    class BulkResponsePromise implements LightblueResponsePromise {
        private final AbstractLightblueDataRequest[] requests;

        BulkResponsePromise(AbstractLightblueDataRequest[] requests) {
            this.requests = requests;
        }

        @Override
        public <T> Future<T> then(
                ResponsesHandler<AbstractLightblueDataRequest, LightblueDataResponse, T> responseHandler) {
            LazyFuture<T> futureResult = new LazyFuture<>(responseHandler);
            queuedRequests.put(futureResult, requests);
            return futureResult;
        }
    }

    static class BulkResponses implements LightblueResponses {
        private final Map<AbstractLightblueDataRequest, LightblueDataResponse> responseMap;

        BulkResponses(Map<AbstractLightblueDataRequest, LightblueDataResponse> responseMap) {
            this.responseMap = responseMap;
        }

        @Override
        public LightblueDataResponse forRequest(AbstractLightblueDataRequest request) {
            return responseMap.get(request);
        }
    }

    class LazyFuture<T> implements Future<T> {
        private final ResponsesHandler<AbstractLightblueDataRequest, LightblueDataResponse, T> responsesHandler;

        private T result;
        private Exception exception;
        private boolean completed;
        private boolean cancelled = false;

        LazyFuture(ResponsesHandler<AbstractLightblueDataRequest, LightblueDataResponse, T> responsesHandler) {
            this.responsesHandler = responsesHandler;
        }

        void complete(LightblueResponses responses) {
            if (isDone()) return;

            completed = true;

            try {
                result = responsesHandler.apply(responses);
            } catch (Exception e) {
                exception = e;
            }
        }

        void completeExceptionally(Exception exception) {
            if (isDone()) return;
            completed = true;
            this.exception = exception;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (completed) return false;
            cancelled = true;
            return true;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isDone() {
            return cancelled || completed;
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            if (cancelled) {
                throw new CancellationException();
            }

            if (completed) {
                doQueuedRequestsAndPopulateResults();
            }

            if (exception != null) {
                throw new ExecutionException(exception);
            }

            return result;
        }

        /**
         * This future is not completed asynchronously so there is no way to "time out" unless we
         * introduce another thread for processing requests which I don't think is hugely necessary.
         */
        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
                TimeoutException {
            return get();
        }
    }
}
