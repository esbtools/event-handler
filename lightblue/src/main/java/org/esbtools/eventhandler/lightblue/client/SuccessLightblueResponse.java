package org.esbtools.eventhandler.lightblue.client;

import com.redhat.lightblue.client.response.LightblueDataResponse;
import com.redhat.lightblue.client.response.LightblueErrorResponse;

public class SuccessLightblueResponse implements LightblueResponse {
    @Override
    public LightblueDataResponse getSuccess() {
        return null;
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    @Override
    public LightblueErrorResponse getFailure() {
        return null;
    }
}
