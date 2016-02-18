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

public class PromiseOfPromise<U> implements Promise<U> {
    private final Promise<Promise<U>> promisedPromise;

    public PromiseOfPromise(Promise<Promise<U>> promisedPromise) {
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
    public U get() throws InterruptedException, ExecutionException {
        return promisedPromise.get().get();
    }

    @Override
    public U get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
        // TODO: Technically, we'd need to track time on first and do delta for second timeout
        // But we don't really care that much right now
        return promisedPromise.get(timeout, unit).get(timeout, unit);
    }

    @Override
    public <V> Promise<V> then(PromiseHandler<U, V> promiseHandler) {
        return promisedPromise.thenPromise(p -> p.then(promiseHandler));
    }

    @Override
    public <V> Promise<V> thenPromise(PromiseHandler<U, Promise<V>> promiseHandler) {
        return promisedPromise.thenPromise(p -> p.thenPromise(promiseHandler));
    }
}
