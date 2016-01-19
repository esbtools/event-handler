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

package org.esbtools.eventhandler.lightblue.testing;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.Locking;
import com.redhat.lightblue.client.request.AbstractDataBulkRequest;
import com.redhat.lightblue.client.request.AbstractLightblueDataRequest;
import com.redhat.lightblue.client.request.LightblueRequest;
import com.redhat.lightblue.client.response.LightblueBulkDataResponse;
import com.redhat.lightblue.client.response.LightblueDataResponse;
import com.redhat.lightblue.client.response.LightblueMetadataResponse;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SlowDataLightblueClient implements LightblueClient {
    private volatile boolean shouldPause = false;
    private volatile Callable<?> request;
    private volatile CompletableFuture<Object> responseFuture = new CompletableFuture<>();

    private final LightblueClient delegate;

    public SlowDataLightblueClient(LightblueClient delegate) {
        this.delegate = delegate;
    }

    public void pauseOnNextRequest() {
        shouldPause = true;
    }

    public void unpause() {
        shouldPause = false;
        flushPendingRequest();
    }

    public void flushPendingRequest() {
        CompletableFuture<Object> currentFuture = responseFuture;
        responseFuture = new CompletableFuture<>();

        try {
            Object response = request.call();
            currentFuture.complete(response);
        } catch (Exception e) {
            currentFuture.completeExceptionally(e);
        }
    }

    @Override
    public Locking getLocking(String s) {
        return delegate.getLocking(s);
    }

    @Override
    public LightblueMetadataResponse metadata(LightblueRequest request) throws LightblueException {
        return delegate.metadata(request);
    }

    @Override
    public LightblueDataResponse data(LightblueRequest request) throws LightblueException {
        return responseOnceFlushedOrLightblueException(() -> delegate.data(request));
    }

    @Override
    public LightblueBulkDataResponse bulkData(
            AbstractDataBulkRequest<AbstractLightblueDataRequest> request)
            throws LightblueException {
        return responseOnceFlushedOrLightblueException(() -> delegate.bulkData(request));
    }

    @Override
    public <T> T data(AbstractLightblueDataRequest request, Class<T> aClass)
            throws LightblueException {
        return responseOnceFlushedOrLightblueException(() -> delegate.data(request, aClass));
    }

    private <T> T responseOnceFlushedOrLightblueException(Callable<T> request)
            throws LightblueException {
        try {
            this.request = request;

            if (shouldPause) {
                return (T) responseFuture.get();
            }

            return request.call();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();

            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }

            if (cause instanceof Error) {
                throw (Error) cause;
            }

            if (cause instanceof LightblueException) {
                throw (LightblueException) cause;
            }

            throw new RuntimeException(e);
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }

            throw new RuntimeException(e);
        }
    }
}
