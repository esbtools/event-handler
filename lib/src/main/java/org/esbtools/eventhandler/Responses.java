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

/**
 * Maps responses to requests. In the case where you make multiple requests, you will likely want
 * to parse each response differently based on the original request. You can get a response for a
 * given request by calling {@link #forRequest(T)}.
 *
 * @param <T> The type of request
 * @param <U> The type of response
 */
public interface Responses<T, U> {
    /**
     * @return The received response for the provided request.
     * @throws java.util.NoSuchElementException If no response for that request exists.
     */
    U forRequest(T request);

    /**
     * @param indexOfRequest Zero-based index of the request based on the original order.
     * @return The received response for the request at the provided index.
     * @throws IndexOutOfBoundsException If the provided index is negative or greater than the
     * number of requests.
     */
    U forRequest(int indexOfRequest);
}
