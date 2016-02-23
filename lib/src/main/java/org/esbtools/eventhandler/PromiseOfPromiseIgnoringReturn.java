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

public class PromiseOfPromiseIgnoringReturn implements Promise<Void> {
    private final Promise<Promise<?>> promisedPromise;

    public PromiseOfPromiseIgnoringReturn(Promise<Promise<?>> promisedPromise) {
        this.promisedPromise = promisedPromise;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return promisedPromise.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return promisedPromise.isCancelled();
    }

    @Override
    public boolean isDone() {
        return promisedPromise.isDone();
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        Promise<?> nextPromise = promisedPromise.get();
        if (nextPromise != null) {
            nextPromise.get();
        }
        return null;
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
        // TODO: Technically, we'd need to track time on first and do delta for second timeout
        // But we don't really care that much right now
        Promise<?> nextPromise = promisedPromise.get(timeout, unit);
        if (nextPromise != null) {
            nextPromise.get(timeout, unit);
        }
        return null;
    }

    @Override
    public <V> Promise<V> then(PromiseHandler<Void, V> promiseHandler) {
        return promisedPromise.thenPromise(p -> p.then(ignored -> promiseHandler.handle(null)));
    }

    @Override
    public <V> Promise<V> thenPromise(PromiseHandler<Void, Promise<V>> promiseHandler) {
        return promisedPromise.thenPromise(p -> p.thenPromise(ignored -> promiseHandler.handle(null)));
    }

    @Override
    public Promise<Void> thenPromiseIgnoringReturn(PromiseHandler<Void, Promise<?>> promiseHandler) {
        return promisedPromise.thenPromise(p ->
                p.thenPromiseIgnoringReturn(ignored -> promiseHandler.handle(null)));
    }
}
