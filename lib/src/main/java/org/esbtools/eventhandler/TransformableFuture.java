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
    static <T> TransformableFuture<T> immediate(T result) {
        return new ImmediateTransformableFuture<>(result);
    }

    static <T> TransformableFuture<T> immediateFailed(Exception exception) {
        return new FailedTransformableFuture<>(exception);
    }

    /**
     * Creates a new {@link Future} which is completed immediately when this {@code Future}
     * completes, with a value that is the result of applying the provided {@code futureTransform}
     * function to this {@code Future}'s result.
     *
     * <p>Terminology inspired by Guava's
     * <a href="https://github.com/google/guava/wiki/ListenableFutureExplained">Futures</a>
     * extensions.
     *
     * @param futureTransform Function which accepts the result from this future and returns a new
     *                        result or throws an exception if a result cannot be computed. This new
     *                        result is what will be returned from the returned {@code Future}.
     * @param <U> The type of new result.
     */
    <U> TransformableFuture<U> transformSync(FutureTransform<T, U> futureTransform);

    /**
     * Creates a new {@code Future} which is backed by the {@code Future} returned from the provided
     * {@code futureTransform} function.
     *
     * <p>Use this when you need to add a callback on an existing {@code TransformableFuture} which
     * returns yet another {@code Future}, chaining several asynchronous actions together into one
     * final result {@code Future}. If you just used {@link #transformSync(FutureTransform)} you'd
     * end up with a {@code Future<Future<U>>} (a future who's result is another future) which is
     * difficult to work with. By using this API, the result {@code Future} is "unwrapped" and you
     * get a more intuitive {@code Future<U>} to work with.
     *
     * <p>Terminology inspired by Guava's
     * <a href="https://github.com/google/guava/wiki/ListenableFutureExplained">Futures</a>
     * extensions such as {@code AsyncFunction}.
     *
     * @param futureTransform Function which accepts the result from this future and returns either
     *                        a new {@code Future} or {@code null}.
     * @param <U> The type of the result within the returned {@code Future} of the transform
     *           function.
     */
    <U> TransformableFuture<U> transformAsync(FutureTransform<T, TransformableFuture<U>> futureTransform);

    /**
     * Like {@link #transformAsync(FutureTransform)}, except useful in cases where you intentionally
     * do not care about the result value contained in the transform functions returned
     * {@code Future}.
     *
     * <p>APIs like {@link Message#process()} may intentionally require the parameterized type of
     * {@code Future<Void>} to clearly document that both the result of the {@code Future} is unused
     * (we only care if it succeeded or not) and to prevent an implementor from accidentally
     * returning a {@code Future<Future>} which would never have its result {@code Future} examined
     * because the consumer is ignoring the return value. This would definitely be a bug.
     */
    TransformableFuture<Void> transformAsyncIgnoringReturn(FutureTransform<T, TransformableFuture<?>> futureTransform);
}
