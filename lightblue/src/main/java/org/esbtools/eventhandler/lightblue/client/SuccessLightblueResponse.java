package org.esbtools.eventhandler.lightblue.client;

import com.redhat.lightblue.client.response.LightblueDataResponse;
import com.redhat.lightblue.client.response.LightblueErrorResponse;

import java.util.NoSuchElementException;

class SuccessLightblueResponse implements LightblueResponse {
    private final LightblueDataResponse response;

    SuccessLightblueResponse(LightblueDataResponse response) {
        this.response = response;
    }

    @Override
    public LightblueDataResponse getSuccess() {
        return response;
    }

    @Override
    public boolean isSuccess() {
        return true;
    }

    @Override
    public LightblueErrorResponse getFailure() {
        throw new NoSuchElementException("There is no error response because the request was " +
                "successful.");
    }
}
