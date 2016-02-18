/*
 *  Copyright 2016 esbtools Contributors and/or its affiliates.
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

package org.esbtools.eventhandler.lightblue.client;

import org.esbtools.eventhandler.Promise;
import org.esbtools.eventhandler.PromiseHandler;
import org.esbtools.eventhandler.PromiseOfPromise;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.model.DataError;
import com.redhat.lightblue.client.model.Error;
import com.redhat.lightblue.client.request.AbstractLightblueDataRequest;
import com.redhat.lightblue.client.request.DataBulkRequest;
import com.redhat.lightblue.client.response.LightblueBulkDataResponse;
import com.redhat.lightblue.client.response.LightblueBulkResponseException;
import com.redhat.lightblue.client.response.LightblueDataResponse;
import com.redhat.lightblue.client.response.LightblueErrorResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A partially thread-safe requester which queues up requests until an associated {@link Future} is
 * resolved, at which point all queued requests are performed in a single batch.
 *
 * <p>This class may be used across multiple threads safely, however the returned {@code Future}s
 * are not threadsafe. That is, a given future instance should not be shared among multiple threads,
 * which should not be a relevant limitation (if it is, however, it would not be hard to make them
 * thread safe). Requests are queued up atomically, and performed and cleared atomically as well.
 * That is, when one future is resolved, the current batch of requests is frozen, copied, cleared,
 * and performed. A thread queueing a request while another thread resolves a future will
 * <em>not</em> result in a loss of requests. It will either make it in for the batch, or be queued
 * for the next.
 *
 * <p>While this class is thread safe, the logical "scope" of returned {@code Future}s is
 * significant to consider. If you know you are going to batch up a bunch of requests, you don't
 * want some other thread interrupting your batch performing your requests before you've finished
 * queueing all of them up. So, you should create a new {@code BulkLightblueRequester} instance per
 * logical "batch," and generally should avoid sharing an instance among multiple threads.
 */
public class BulkLightblueRequester implements LightblueRequester {
    private final LightblueClient lightblue;
    private final Map<LazyPromise, AbstractLightblueDataRequest[]> queuedRequests =
            Collections.synchronizedMap(new HashMap<>());

    public BulkLightblueRequester(LightblueClient lightblue) {
        this.lightblue = lightblue;
    }

    @Override
    public Promise<LightblueResponses> request(AbstractLightblueDataRequest... requests) {
        LazyPromise<LightblueResponses, LightblueResponses> responsePromise =
                new LazyPromise<>(t -> t);
        queuedRequests.put(responsePromise, requests);
        return responsePromise;
    }

    private void doQueuedRequestsAndCompleteFutures() {
        Map<LazyPromise, AbstractLightblueDataRequest[]> batch;

        synchronized (queuedRequests) {
            batch = new HashMap<>(queuedRequests);
            queuedRequests.clear();
        }

        DataBulkRequest bulkRequest = new DataBulkRequest();

        for (AbstractLightblueDataRequest[] allRequestBatches : batch.values()) {
            for (AbstractLightblueDataRequest requestInBatch : allRequestBatches) {
                // TODO: Determine if any requests are equivalent / duplicated and filter out
                bulkRequest.add(requestInBatch);
            }
        }

        try {
            LightblueBulkDataResponse bulkResponse = tryBulkRequest(bulkRequest);

            for (Entry<LazyPromise, AbstractLightblueDataRequest[]> lazyFutureToRequests : batch.entrySet()) {
                LazyPromise lazyPromise = lazyFutureToRequests.getKey();
                AbstractLightblueDataRequest[] requests = lazyFutureToRequests.getValue();
                Map<AbstractLightblueDataRequest, LightblueDataResponse> responseMap =
                        new HashMap<>(requests.length);
                List<Error> errors = new ArrayList<>();

                for (AbstractLightblueDataRequest request : requests) {
                    LightblueDataResponse response = bulkResponse.getResponse(request);

                    if (response instanceof LightblueErrorResponse) {
                        LightblueErrorResponse errorResponse = (LightblueErrorResponse) response;

                        DataError[] dataErrors = errorResponse.getDataErrors();
                        Error[] lightblueErrors = errorResponse.getLightblueErrors();

                        if (dataErrors != null) {
                            for (DataError dataError : dataErrors) {
                                errors.addAll(dataError.getErrors());
                            }
                        }

                        if (lightblueErrors != null) {
                            Collections.addAll(errors, lightblueErrors);
                        }
                    }

                    responseMap.put(request, response);
                }

                if (errors.isEmpty()) {
                    lazyPromise.complete(new BulkResponses(responseMap));
                } else {
                    lazyPromise.completeExceptionally(new BulkLightblueResponseException(errors));
                }
            }
        } catch (LightblueException e) {
            for (LazyPromise batchedPromise : batch.keySet()) {
                batchedPromise.completeExceptionally(e);
            }
        }
    }

    /**
     * Swallows exceptions related to errors in individual requests on purpose. The returned
     * bulk response object may have failed responses.
     *
     * @throws LightblueException if something else went wrong, in which case there is no usable
     *                            response at all.
     */
    private LightblueBulkDataResponse tryBulkRequest(DataBulkRequest bulkRequest) throws LightblueException {
        try {
            return lightblue.bulkData(bulkRequest);
        } catch (LightblueBulkResponseException e) {
            return e.getBulkResponse();
        }
    }

    static class BulkResponses implements LightblueResponses {
        private final Map<AbstractLightblueDataRequest, LightblueDataResponse> responseMap;

        BulkResponses(Map<AbstractLightblueDataRequest, LightblueDataResponse> responseMap) {
            this.responseMap = responseMap;
        }

        @Override
        public LightblueDataResponse forRequest(AbstractLightblueDataRequest request) {
            if (responseMap.containsKey(request)) {
                return responseMap.get(request);
            }

            throw new NoSuchElementException("No response for request: " + request);
        }
    }

    class LazyPromise<T, U> implements Promise<U> {
        private final PromiseHandler<T, U> handler;

        private U result;
        private Exception exception;
        private boolean completed;
        private boolean cancelled = false;

        /** Promises that we need to complete once this one is completed. */
        final List<LazyPromise<U, ?>> next = new ArrayList<>(1);

        LazyPromise(PromiseHandler<T, U> handler) {
            this.handler = handler;
        }

        void complete(T responses) {
            if (isDone()) return;

            try {
                result = handler.handle(responses);
                completed = true;
                for (LazyPromise<U, ?> next : this.next) {
                    next.complete(result);
                }
            } catch (Exception e) {
                completeExceptionally(e);
            }
        }

        void completeExceptionally(Exception exception) {
            if (isDone()) return;
            completed = true;
            this.exception = exception;
            for (LazyPromise<U, ?> next : this.next) {
                next.completeExceptionally(exception);
            }
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
        public U get() throws InterruptedException, ExecutionException {
            if (cancelled) {
                throw new CancellationException();
            }

            if (!completed) {
                doQueuedRequestsAndCompleteFutures();
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
        public U get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
                TimeoutException {
            return get();
        }

        @Override
        public <V> Promise<V> then(PromiseHandler<U, V> promiseHandler) {
            LazyPromise<U, V> promise = new LazyPromise<>(promiseHandler);
            next.add(promise);
            return promise;
        }

        @Override
        public <V> Promise<V> thenPromise(PromiseHandler<U, Promise<V>> promiseHandler) {
            LazyPromise<U, Promise<V>> promise = new LazyPromise<>(promiseHandler);
            next.add(promise);
            return new PromiseOfPromise<>(promise);
        }
    }

}
