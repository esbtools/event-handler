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

package org.esbtools.eventhandler.lightblue;

import com.redhat.lightblue.client.model.DataError;
import com.redhat.lightblue.client.model.Error;
import com.redhat.lightblue.client.response.LightblueErrorResponse;
import com.redhat.lightblue.client.response.LightblueResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public abstract class LightblueErrors {
    public static boolean arePresentInResponse(LightblueResponse response) {
        if (response instanceof LightblueErrorResponse) {
            LightblueErrorResponse errorResponse = (LightblueErrorResponse) response;

            // Likely transient failure; leave event alone to be tried again later.
            return errorResponse.hasDataErrors() || errorResponse.hasLightblueErrors();
        }

        return false;
    }

    public static List<String> toStringsFromErrorResponse(LightblueResponse response) {
        if (!(response instanceof LightblueErrorResponse)) {
            return Collections.emptyList();
        }

        LightblueErrorResponse errorResponse = (LightblueErrorResponse) response;
        List<Error> errors = new ArrayList<>();

        Error[] lightblueErrors = errorResponse.getLightblueErrors();
        DataError[] dataErrors = errorResponse.getDataErrors();

        if (lightblueErrors != null) {
            Collections.addAll(errors, lightblueErrors);
        }

        if (dataErrors != null) {
            Collections.addAll(errors, Arrays.stream(dataErrors)
                    .flatMap(dataError -> dataError.getErrors().stream())
                    .toArray(Error[]::new));
        }

        return errors.stream()
                .map(e -> "Code: " + e.getErrorCode() + ", " +
                        "Context: " + e.getContext() + ", " +
                        "Message: " + e.getMsg())
                .collect(Collectors.toList());
    }
}
