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
 * An extension of {@link Future} which allows chaining {@link FutureTransform} functions onto its
 * eventually computed result, returning new {@code TransformableFuture}s.
 *
 * <p>Not a general purpose interface, this is entirely meant to optionally support integrating with
 * {@code Future}-based APIs like {@link DocumentEvent#lookupDocument()} in collaboration with a
 * {@link Requester}.
 */
public interface TransformableFuture<T> extends Future<T> {
    /**
     * Once responses are received from some requests, the provided {@code responseHandler} function
     * will be called with those responses. This handler returns a value that is used for the
     * returned {@link Future}'s {@link Future#get() get} methods.
     *
     * @param futureTransform Function which accepts responses and returns a result or throws an
     *                        exception if a result cannot be computed.
     * @param <U> The type of result.
     */
    <U> TransformableFuture<U> transformSync(FutureTransform<T, U> futureTransform);

    <U> TransformableFuture<U> transformAsync(FutureTransform<T, TransformableFuture<U>> futureTransform);

    TransformableFuture<Void> transformAsyncIgnoringReturn(FutureTransform<T, TransformableFuture<?>> futureTransform);
}
