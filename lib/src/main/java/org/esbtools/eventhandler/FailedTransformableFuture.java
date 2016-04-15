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

package org.esbtools.eventhandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class FailedTransformableFuture<T> implements TransformableFuture<T> {
    private final Exception exception;

    private static final Logger log = LoggerFactory.getLogger(FailedTransformableFuture.class);

    public FailedTransformableFuture(Exception exception) {
        this.exception = exception;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        throw new ExecutionException(exception);
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
        throw new ExecutionException(exception);
    }

    @Override
    public <U> TransformableFuture<U> transformSync(FutureTransform<T, U> futureTransform) {
        return new FailedTransformableFuture<>(exception);
    }

    @Override
    public <U> TransformableFuture<U> transformAsync(
            FutureTransform<T, TransformableFuture<U>> futureTransform) {
        return new FailedTransformableFuture<>(exception);
    }

    @Override
    public TransformableFuture<Void> transformAsyncIgnoringReturn(
            FutureTransform<T, TransformableFuture<?>> futureTransform) {
        return new FailedTransformableFuture<>(exception);
    }

    @Override
    public TransformableFuture<T> whenDoneOrCancelled(FutureDoneCallback callback) {
        try {
            callback.onDoneOrCancelled();
        } catch (Exception e) {
            log.warn("Exception caught and ignored while running future done callback.", e);
        }

        return this;
    }
}
