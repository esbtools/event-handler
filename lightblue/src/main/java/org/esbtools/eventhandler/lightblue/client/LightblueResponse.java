package org.esbtools.eventhandler.lightblue.client;

import com.redhat.lightblue.client.model.DataError;
import com.redhat.lightblue.client.model.Error;
import com.redhat.lightblue.client.response.LightblueDataResponse;
import com.redhat.lightblue.client.response.LightblueErrorResponse;

import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Represents a lightblue response which may or may not have been successful.
 *
 * <p>To wrap a response from the lightblue client library, see
 * {@link #fromClientResponse(com.redhat.lightblue.client.response.LightblueResponse)}
 *
 * <p>Typical use cases vary on how you would handle a failed response
 * <ul>
 *     <li>"I would throw an exception": In this case just use {@link #getSuccess()} and let the
 *     exception propagate if it failed.</li>
 *     <li>"I would examine the errors before doing something else": In this case call
 *     {@link #isSuccess()} first, and if {@code false} use the response from {@link #getFailure()}
 *     </li>
 * </ul>
 */
public interface LightblueResponse {
    /**
     * Returns the response if it was successful as a {@link LightblueDataResponse} matching the
     * successful lightblue response schema. If the response was not successful, then an unchecked
     * {@link LightblueResponseException} is thrown with the lightblue errors in the response.
     *
     * <p>You should query if the response was successful or not first with {@link #isSuccess()},
     * unless all you would do is throw an exception if it failed. In that case, you could just call
     * {@code getSuccess()} unconditionally and let the exception propagate.
     *
     * @throws LightblueResponseException if this response is not a successful one. Check if the
     * response is successful first with {@link #isSuccess()}.
     */
    LightblueDataResponse getSuccess() throws LightblueResponseException;

    /**
     * This response can represent both a successful and failed one. Use this to query which it is.
     */
    boolean isSuccess();

    /**
     * @return A failed lightblue response with one or more errors.
     * @throws java.util.NoSuchElementException if this response is not a failed one. Check if the
     * response is successful first with {@link #isSuccess()}.
     */
    LightblueErrorResponse getFailure() throws NoSuchElementException;

    /**
     * Wraps a response from the lightblue client library which may or may not be successful in a
     * type which allows easily querying whether or not the response was successful and getting at
     * the right type of response.
     */
    static LightblueResponse fromClientResponse(
            com.redhat.lightblue.client.response.LightblueResponse response) {
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
