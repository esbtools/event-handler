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

import com.redhat.lightblue.client.model.DataError;
import com.redhat.lightblue.client.model.Error;
import com.redhat.lightblue.client.response.LightblueDataResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class LightblueErrorDocument {
    private final List<Error> errors;
    private final List<DataError> dataErrors;
    private final List<LightblueDataResponse> successfulResponses;

    public LightblueErrorDocument(List<Error> errors, List<DataError> dataErrors,
            List<LightblueDataResponse> successfulResponses) {
        this.errors = new ArrayList<>(errors);
        this.dataErrors = new ArrayList<>(dataErrors);
        this.successfulResponses = new ArrayList<>(successfulResponses);
    }

    public List<Error> getErrors() {
        return errors;
    }

    public List<DataError> getDataErrors() {
        return dataErrors;
    }

    public List<LightblueDataResponse> getSuccessfulResponses() {
        return successfulResponses;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LightblueErrorDocument that = (LightblueErrorDocument) o;
        return Objects.equals(errors, that.errors) &&
                Objects.equals(dataErrors, that.dataErrors) &&
                Objects.equals(successfulResponses, that.successfulResponses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(errors, dataErrors, successfulResponses);
    }

    @Override
    public String toString() {
        return "LightblueErrorDocument{" +
                "errors=" + errors +
                ", dataErrors=" + dataErrors +
                ", successfulResponses=" + successfulResponses +
                '}';
    }
}
