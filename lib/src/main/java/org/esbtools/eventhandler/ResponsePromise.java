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

import java.util.concurrent.Future;

/**
 * An asynchronous capture of potentially yet-to-come responses.
 *
 * <p>Allows code to build a {@link Future} object which may populate lazily with results from the
 * {@link ResponsesHandler} passed to {@link #then(ResponsesHandler)}.
 *
 * @param <T> The type of requests
 * @param <U> The type of responses
 */
public interface ResponsePromise<T, U> {
    /**
     * Once responses are received from some requests, the provided {@code responseHandler} function
     * will be called with those responses. This handler returns a value that is used for the
     * returned {@link Future}'s {@link Future#get() get} methods.
     *
     * @param responseHandler Function which accepts responses and returns a result or throws an
     *                        exception if a result cannot be computed.
     * @param <V> The type of result.
     */
    <V> Future<V> then(ResponsesHandler<T, U, V> responseHandler);
}
