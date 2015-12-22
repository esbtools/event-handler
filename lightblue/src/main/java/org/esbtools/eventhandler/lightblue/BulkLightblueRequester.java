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
import com.redhat.lightblue.client.http.LightblueHttpClientException;
import com.redhat.lightblue.client.model.DataError;
import com.redhat.lightblue.client.model.Error;
import com.redhat.lightblue.client.request.AbstractLightblueDataRequest;
import com.redhat.lightblue.client.request.DataBulkRequest;
import com.redhat.lightblue.client.response.LightblueBulkDataResponse;
import com.redhat.lightblue.client.response.LightblueDataResponse;
import com.redhat.lightblue.client.response.LightblueErrorResponse;
import com.redhat.lightblue.client.response.LightblueException;
import com.redhat.lightblue.client.response.LightblueResponse;

import org.esbtools.eventhandler.Responses;
import org.esbtools.eventhandler.ResponsesHandler;
import org.esbtools.eventhandler.Result;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class BulkLightblueRequester implements LightblueRequester {
    private final LightblueClient lightblue;
    private final Map<LazyResult, AbstractLightblueDataRequest[]> queuedRequests =
            Collections.synchronizedMap(new HashMap<>());

    public BulkLightblueRequester(LightblueClient lightblue) {
        this.lightblue = lightblue;
    }

    @Override
    public LightblueResponsePromise queueRequests(AbstractLightblueDataRequest... requests) {
        return new BulkResponsePromise(requests);
    }

    private void doQueuedRequestsAndPopulateResults() {
        Map<LazyResult, AbstractLightblueDataRequest[]> batch;

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

            for (Entry<LazyResult, AbstractLightblueDataRequest[]> lazyResultToRequests : batch.entrySet()) {
                LazyResult lazyResult = lazyResultToRequests.getKey();
                AbstractLightblueDataRequest[] requests = lazyResultToRequests.getValue();
                Map<AbstractLightblueDataRequest, LightblueDataResponse> responseMap = new HashMap<>(requests.length);
                List<String> errors = new ArrayList<>();

                for (AbstractLightblueDataRequest request : requests) {
                    LightblueResponse response = bulkResponse.getResponse(request);

                    if (response instanceof LightblueErrorResponse) {
                        LightblueErrorResponse errorResponse = (LightblueErrorResponse) response;

                        for (DataError dataError : errorResponse.getDataErrors()) {
                            for (Error error : dataError.getErrors()) {
                                errors.add(error.getMsg());
                            }
                        }

                        for (Error error : errorResponse.getLightblueErrors()) {
                            errors.add(error.getMsg());
                        }
                    }

                    if (response instanceof LightblueDataResponse) {
                        responseMap.put(request, (LightblueDataResponse) response);
                    }
                }

                if (errors.isEmpty()) {
                    lazyResult.complete(new BulkResponses(responseMap));
                } else {
                    lazyResult.completeWithErrors(errors);
                }
            }
        } catch (LightblueException e) {
            Collection<String> errorMessages = makeErrorMessagesFromLightblueException(e);

            for (Entry<LazyResult, AbstractLightblueDataRequest[]> lazyResultToRequests : batch.entrySet()) {
                LazyResult lazyResult = lazyResultToRequests.getKey();
                lazyResult.completeWithErrors(errorMessages);
            }
        }
    }

    private static Collection<String> makeErrorMessagesFromLightblueException(LightblueException e) {
        if (e instanceof LightblueHttpClientException) {
            LightblueHttpClientException httpException = (LightblueHttpClientException) e;

            return Collections.singletonList(httpException.getHttpResponseBody());
        }

        return Collections.singleton(e.getMessage());
    }

    class BulkResponsePromise implements LightblueResponsePromise {
        private final AbstractLightblueDataRequest[] requests;

        BulkResponsePromise(AbstractLightblueDataRequest[] requests) {
            this.requests = requests;
        }

        @Override
        public <T> LazyResult<T> then(
                ResponsesHandler<AbstractLightblueDataRequest, LightblueDataResponse, T> responseHandler) {
            LazyResult<T> futureResult = new LazyResult<>(responseHandler);
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

    class LazyResult<T> implements Result<T> {
        private final ResponsesHandler<AbstractLightblueDataRequest, LightblueDataResponse, T> responsesHandler;
        private final List<String> errors = new ArrayList<>();

        private T result;
        private boolean handled;

        LazyResult(ResponsesHandler<AbstractLightblueDataRequest, LightblueDataResponse, T> responsesHandler) {
            this.responsesHandler = responsesHandler;
        }

        void complete(Responses<AbstractLightblueDataRequest, LightblueDataResponse> responses) {
            if (handled) return; // Should never happen.

            handled = true;

            try {
                result = responsesHandler.apply(responses);
            } catch (Exception e) {
                StringWriter stringWriter = new StringWriter();
                PrintWriter writer = new PrintWriter(stringWriter);
                e.printStackTrace(writer);
                errors.add(stringWriter.toString());
            }
        }

        void completeWithErrors(Collection<String> errors) {
            handled = true;

            this.errors.addAll(errors);
        }

        @Override
        public T get() {
            if (!handled) {
                doQueuedRequestsAndPopulateResults();
            }

            return result;
        }

        @Override
        public Collection<String> errors() {
            if (!handled) {
                doQueuedRequestsAndPopulateResults();
            }

            return Collections.unmodifiableCollection(errors);
        }
    }
}
