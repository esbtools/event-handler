package org.esbtools.eventhandler.lightblue.client;

import com.redhat.lightblue.client.model.DataError;
import com.redhat.lightblue.client.model.Error;
import com.redhat.lightblue.client.response.LightblueDataResponse;
import com.redhat.lightblue.client.response.LightblueErrorResponse;

import java.util.Optional;

// FIXME: name better?
public interface LightblueResponse {
    LightblueDataResponse getSuccess() ; // throw Lightblue*Exception
    boolean isSuccess();
    LightblueErrorResponse getFailure(); // throws MissingFailureResponse or NoSuchElement

    static LightblueResponse get(com.redhat.lightblue.client.response.LightblueResponse response) {
        if (response instanceof LightblueErrorResponse) {
            LightblueErrorResponse errorResponse = (LightblueErrorResponse) response;

            DataError[] dataErrors = Optional.ofNullable(errorResponse.getDataErrors())
                    .orElse(new DataError[0]);
            Error[] lightblueErrors = Optional.ofNullable(errorResponse.getLightblueErrors())
                    .orElse(new Error[0]);

            if (dataErrors.length + lightblueErrors.length > 0) {
                return new FailedLightblueResponse(errorResponse, dataErrors, lightblueErrors);
            }
        }

        if (response instanceof LightblueDataResponse) {
            return new SuccessLightblueResponse((LightblueDataResponse) response);
        }

        throw new IllegalArgumentException("LightblueResponse was neither a data response nor an " +
                "error response.");
    }
}
