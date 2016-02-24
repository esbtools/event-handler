/*
 *  Copyright 2016 esbtools Contributors and/or its affiliates.
 *
 *  This file is part of esbtools.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNVoid General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOVoidT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICVoidLAR PVoidRPOSE.  See the
 *  GNVoid General Public License for more details.
 *
 *  You should have received a copy of the GNVoid General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.esbtools.eventhandler;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class NestedTransformableFutureIgnoringReturn implements TransformableFuture<Void> {
    private final TransformableFuture<TransformableFuture<?>> promisedTransformableFuture;

    public NestedTransformableFutureIgnoringReturn(TransformableFuture<TransformableFuture<?>> promisedTransformableFuture) {
        this.promisedTransformableFuture = promisedTransformableFuture;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return promisedTransformableFuture.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return promisedTransformableFuture.isCancelled();
    }

    @Override
    public boolean isDone() {
        return promisedTransformableFuture.isDone();
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        TransformableFuture<?> nextTransformableFuture = promisedTransformableFuture.get();
        if (nextTransformableFuture != null) {
            nextTransformableFuture.get();
        }
        return null;
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
        // TODO: Technically, we'd need to track time on first and do delta for second timeout
        // But we don't really care that much right now
        TransformableFuture<?> nextFuture = promisedTransformableFuture.get(timeout, unit);
        if (nextFuture != null) {
            nextFuture.get(timeout, unit);
        }
        return null;
    }

    @Override
    public <V> TransformableFuture<V> transformSync(FutureTransform<Void, V> futureTransform) {
        return promisedTransformableFuture.transformAsync(p ->
                p.transformSync(ignored -> futureTransform.handle(null)));
    }

    @Override
    public <V> TransformableFuture<V> transformAsync(
            FutureTransform<Void, TransformableFuture<V>> futureTransform) {
        return promisedTransformableFuture.transformAsync(p ->
                p.transformAsync(ignored -> futureTransform.handle(null)));
    }

    @Override
    public TransformableFuture<Void> transformAsyncIgnoringReturn(
            FutureTransform<Void, TransformableFuture<?>> futureTransform) {
        return promisedTransformableFuture.transformAsync(p ->
                p.transformAsyncIgnoringReturn(ignored -> futureTransform.handle(null)));
    }
}
