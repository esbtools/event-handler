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

import org.esbtools.eventhandler.Requester;
import org.esbtools.eventhandler.Responses;
import org.esbtools.eventhandler.TransformableFuture;

import com.redhat.lightblue.client.request.AbstractLightblueDataRequest;
import com.redhat.lightblue.client.response.LightblueDataResponse;

import java.util.Collection;

public interface LightblueRequester extends
        Requester<AbstractLightblueDataRequest, LightblueDataResponse> {

    /**
     * Like {@link #request(Object[])}, except with relaxed failure conditions so that listeners to
     * this future result may examine and transform errors from lightblue in addition to successful
     * responses. A listener can examine whether or not these responses are failed with
     * {@link LightblueResponse#isSuccess()}.
     *
     * <p>The returned future will only fail if the lightblue request did not complete and there are
     * no responses with which to populate the future value.
     *
     * <p>This contrasts with {@link #request(Object[])} which always returns successful
     * {@link LightblueDataResponse}s, and if any responses have returned with errors, the future
     * will fail.
     *
     * @see LightblueResponse
     */
    TransformableFuture<? extends Responses<AbstractLightblueDataRequest, LightblueResponse>>
            tryRequest(AbstractLightblueDataRequest... req);

    /**
     * Like {@link #request(Collection)}, except with relaxed failure conditions so that listeners
     * to this future result may examine and transform errors from lightblue in addition to
     * successful responses. A listener can examine whether or not these responses are failed with
     * {@link LightblueResponse#isSuccess()}.
     *
     * <p>The returned future will only fail if the lightblue request did not complete and there are
     * no responses with which to populate the future value.
     *
     * <p>This contrasts with {@link #request(Collection)} which always returns successful
     * {@link LightblueDataResponse}s, and if any responses have returned with errors, the future
     * will fail.
     *
     * @see LightblueResponse
     */
    default TransformableFuture<? extends Responses<AbstractLightblueDataRequest, LightblueResponse>>
            tryRequest(Collection<? extends AbstractLightblueDataRequest> req) {
        return tryRequest(req.stream().toArray(AbstractLightblueDataRequest[]::new));
    }
}
