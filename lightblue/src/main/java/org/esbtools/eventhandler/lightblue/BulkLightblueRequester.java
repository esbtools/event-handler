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

import java.util.Collections;
import java.util.HashMap;
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

    private void doQueuedRequestsAndPopulateResults() throws LightblueException {
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

        LightblueBulkDataResponse bulkResponse = lightblue.bulkData(bulkRequest);

        for (Entry<LazyResult, AbstractLightblueDataRequest[]> lazyResultToRequests : batch.entrySet()) {
            LazyResult lazyResult = lazyResultToRequests.getKey();
            AbstractLightblueDataRequest[] requests = lazyResultToRequests.getValue();
            Map<AbstractLightblueDataRequest, LightblueDataResponse> responseMap = new HashMap<>(requests.length);

            for (AbstractLightblueDataRequest request : requests) {
                LightblueResponse response = bulkResponse.getResponse(request);

                if (response instanceof LightblueErrorResponse) {
                    LightblueErrorResponse errorResponse = (LightblueErrorResponse) response;
                    if (errorResponse.hasLightblueErrors() || errorResponse.hasDataErrors()) {
                        // TODO: handle errors
                        continue;
                    }
                }

                if (response instanceof LightblueDataResponse) {
                    responseMap.put(request, (LightblueDataResponse) response);
                }
            }

            lazyResult.handleResponses(new BulkResponses(responseMap));
        }
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
        private T result;
        private boolean handled;

        LazyResult(ResponsesHandler<AbstractLightblueDataRequest, LightblueDataResponse, T> responsesHandler) {
            this.responsesHandler = responsesHandler;
        }

        void handleResponses(Responses<AbstractLightblueDataRequest, LightblueDataResponse> responses) {
            if (handled) return; // Should never happen.
            handled = true;
            // TODO: handle exception
            // TODO: generics refactoring?
            result = responsesHandler.apply(responses);
        }

        @Override
        public T get() {
            if (handled) {
                return result;
            }

            try {
                doQueuedRequestsAndPopulateResults();
            } catch (LightblueException e) {
                // TODO: handle
                e.printStackTrace();
            }

            return result;
        }
    }
}
