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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class ImmediateTransformableFuture<T> implements TransformableFuture<T> {
    private final T result;

    public ImmediateTransformableFuture(T result) {
        this.result = result;
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
        return result;
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
        return result;
    }

    @Override
    public <U> TransformableFuture<U> transformSync(FutureTransform<T, U> futureTransform) {
        try {
            return new ImmediateTransformableFuture<>(futureTransform.handle(result));
        } catch (Exception e) {
            return new FailedTransformableFuture<>(e);
        }
    }

    @Override
    public <U> TransformableFuture<U> transformAsync(
            FutureTransform<T, TransformableFuture<U>> futureTransform) {
        try {
            return futureTransform.handle(result);
        } catch (Exception e) {
            return new FailedTransformableFuture<>(e);
        }
    }

    @Override
    public TransformableFuture<Void> transformAsyncIgnoringReturn(
            FutureTransform<T, TransformableFuture<?>> futureTransform) {
        try {
            return (TransformableFuture<Void>) futureTransform.handle(result);
        } catch (Exception e) {
            return new FailedTransformableFuture<>(e);
        }
    }
}
