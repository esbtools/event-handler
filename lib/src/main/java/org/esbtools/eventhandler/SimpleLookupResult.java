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

package org.esbtools.eventhandler;

import java.util.Objects;

public final class SimpleLookupResult implements LookupResult {
    private final Object body;
    private final boolean hasErrors;
    private final DocumentEvent associatedEvent;

    public static SimpleLookupResult withoutErrors(Object body, DocumentEvent associatedEvent) {
        return new SimpleLookupResult(body, false, associatedEvent);
    }

    public static SimpleLookupResult withErrors(Object body, DocumentEvent associatedEvent) {
        return new SimpleLookupResult(body, true, associatedEvent);
    }

    private SimpleLookupResult(Object body, boolean hasErrors, DocumentEvent associatedEvent) {
        this.body = body;
        this.hasErrors = hasErrors;
        this.associatedEvent = associatedEvent;
    }

    @Override
    public Object getBody() {
        return body;
    }

    @Override
    public boolean hasErrors() {
        return hasErrors;
    }

    @Override
    public DocumentEvent getAssociatedEvent() {
        return associatedEvent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleLookupResult that = (SimpleLookupResult) o;
        return hasErrors == that.hasErrors &&
                Objects.equals(body, that.body) &&
                Objects.equals(associatedEvent, that.associatedEvent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(body, hasErrors, associatedEvent);
    }

    @Override
    public String toString() {
        return "LookupResult{" +
                "body=" + body +
                ", hasErrors=" + hasErrors +
                ", associatedEvent=" + associatedEvent +
                '}';
    }
}
