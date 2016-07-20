package org.esbtools.eventhandler.lightblue.client;

import com.redhat.lightblue.client.model.DataError;
import com.redhat.lightblue.client.model.Error;
import com.redhat.lightblue.client.response.LightblueDataResponse;
import com.redhat.lightblue.client.response.LightblueErrorResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class FailedLightblueResponse implements LightblueResponse {
    private final LightblueErrorResponse response;
    private final List<Error> errors = new ArrayList<>();

    FailedLightblueResponse(LightblueErrorResponse response,
            DataError[] dataErrors, Error[] lightblueErrors) {
        this.response = response;

        Arrays.stream(dataErrors)
                .flatMap(e -> e.getErrors().stream())
                .forEach(errors::add);
        Collections.addAll(errors, lightblueErrors);
    }

    @Override
    public LightblueDataResponse getSuccess() {
        // TODO: maybe dif exception?
        throw new LightblueResponseException(errors);
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    @Override
    public LightblueErrorResponse getFailure() {
        return response;
    }
}
