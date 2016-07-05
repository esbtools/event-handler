package org.esbtools.eventhandler.lightblue.client;

import com.redhat.lightblue.client.response.LightblueDataResponse;
import com.redhat.lightblue.client.response.LightblueErrorResponse;

// FIXME: name better
public interface LightblueResponse {
    LightblueDataResponse getSuccess(); // throw Lightblue*Exception
    boolean isSuccess();
    LightblueErrorResponse getFailure(); // throws MissingFailureResponse or NoSuchElement
}
