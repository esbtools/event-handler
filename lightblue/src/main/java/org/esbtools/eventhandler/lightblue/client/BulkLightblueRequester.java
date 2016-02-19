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

package org.esbtools.eventhandler.lightblue.client;

import org.esbtools.eventhandler.Promise;
import org.esbtools.eventhandler.PromiseHandler;
import org.esbtools.eventhandler.PromiseOfPromise;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.model.DataError;
import com.redhat.lightblue.client.model.Error;
import com.redhat.lightblue.client.request.AbstractLightblueDataRequest;
import com.redhat.lightblue.client.request.DataBulkRequest;
import com.redhat.lightblue.client.response.LightblueBulkDataResponse;
import com.redhat.lightblue.client.response.LightblueBulkResponseException;
import com.redhat.lightblue.client.response.LightblueDataResponse;
import com.redhat.lightblue.client.response.LightblueErrorResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A partially thread-safe requester which queues up requests until an associated {@link Future} is
 * resolved, at which point all queued requests are performed in a single batch.
 *
 * <p>This class may be used across multiple threads safely, however the returned {@code Future}s
 * are not threadsafe. That is, a given future instance should not be shared among multiple threads,
 * which should not be a relevant limitation (if it is, however, it would not be hard to make them
 * thread safe). Requests are queued up atomically, and performed and cleared atomically as well.
 * That is, when one future is resolved, the current batch of requests is frozen, copied, cleared,
 * and performed. A thread queueing a request while another thread resolves a future will
 * <em>not</em> result in a loss of requests. It will either make it in for the batch, or be queued
 * for the next.
 *
 * <p>While this class is thread safe, the logical "scope" of returned {@code Future}s is
 * significant to consider. If you know you are going to batch up a bunch of requests, you don't
 * want some other thread interrupting your batch performing your requests before you've finished
 * queueing all of them up. So, you should create a new {@code BulkLightblueRequester} instance per
 * logical "batch," and generally should avoid sharing an instance among multiple threads.
 */
public class BulkLightblueRequester implements LightblueRequester {
    private final LightblueClient lightblue;
    private final List<LazyRequestPromise> queuedRequests =
            Collections.synchronizedList(new ArrayList<>());

    public BulkLightblueRequester(LightblueClient lightblue) {
        this.lightblue = lightblue;
    }

    @Override
    public Promise<LightblueResponses> request(AbstractLightblueDataRequest... requests) {
        LazyRequestPromise responsePromise = new LazyRequestPromise(requests);
        queuedRequests.add(responsePromise);
        return responsePromise;
    }

    private void doQueuedRequestsAndCompleteFutures() {
        List<LazyRequestPromise> batch;

        synchronized (queuedRequests) {
            batch = new ArrayList<>(queuedRequests);
            queuedRequests.clear();
        }

        DataBulkRequest bulkRequest = new DataBulkRequest();

        for (LazyRequestPromise batchedPromise : batch) {
            for (AbstractLightblueDataRequest requestInBatch : batchedPromise.requests) {
                // TODO: Determine if any requests are equivalent / duplicated and filter out
                bulkRequest.add(requestInBatch);
            }
        }

        try {
            LightblueBulkDataResponse bulkResponse = tryBulkRequest(bulkRequest);

            for (LazyRequestPromise batchedPromise : batch) {
                AbstractLightblueDataRequest[] requests = batchedPromise.requests;
                Map<AbstractLightblueDataRequest, LightblueDataResponse> responseMap =
                        new HashMap<>(requests.length);
                List<Error> errors = new ArrayList<>();

                for (AbstractLightblueDataRequest request : requests) {
                    LightblueDataResponse response = bulkResponse.getResponse(request);

                    if (response instanceof LightblueErrorResponse) {
                        LightblueErrorResponse errorResponse = (LightblueErrorResponse) response;

                        DataError[] dataErrors = errorResponse.getDataErrors();
                        Error[] lightblueErrors = errorResponse.getLightblueErrors();

                        if (dataErrors != null) {
                            for (DataError dataError : dataErrors) {
                                errors.addAll(dataError.getErrors());
                            }
                        }

                        if (lightblueErrors != null) {
                            Collections.addAll(errors, lightblueErrors);
                        }
                    }

                    responseMap.put(request, response);
                }

                if (errors.isEmpty()) {
                    batchedPromise.complete(new BulkResponses(responseMap));
                } else {
                    batchedPromise.completeExceptionally(new BulkLightblueResponseException(errors));
                }
            }
        } catch (LightblueException e) {
            for (LazyRequestPromise batchedPromise : batch) {
                batchedPromise.completeExceptionally(e);
            }
        }
    }

    /**
     * Swallows exceptions related to errors in individual requests on purpose. The returned
     * bulk response object may have failed responses.
     *
     * @throws LightblueException if something else went wrong, in which case there is no usable
     *                            response at all.
     */
    private LightblueBulkDataResponse tryBulkRequest(DataBulkRequest bulkRequest)
            throws LightblueException {
        try {
            return lightblue.bulkData(bulkRequest);
        } catch (LightblueBulkResponseException e) {
            return e.getBulkResponse();
        }
    }

    static class BulkResponses implements LightblueResponses {
        private final Map<AbstractLightblueDataRequest, LightblueDataResponse> responseMap;

        BulkResponses(Map<AbstractLightblueDataRequest, LightblueDataResponse> responseMap) {
            this.responseMap = responseMap;
        }

        @Override
        public LightblueDataResponse forRequest(AbstractLightblueDataRequest request) {
            if (responseMap.containsKey(request)) {
                return responseMap.get(request);
            }

            throw new NoSuchElementException("No response for request: " + request);
        }
    }

    /**
     * A core promise implementation which accepts its result from a caller, triggering this result
     * lazily on the first call to {@link #get()}.
     *
     * <p>This promise is used to back more specific cases in {@link LazyRequestPromise} and
     * {@link HandlingPromise}.
     *
     * <p>Promises (and {@link Future}s) typically work asynchronously in another thread. This
     * implementation intentionally does not. To reap the performance benefits of batching requests,
     * we need a promise design which puts off doing <em>any</em> work as long as possible: until
     * something calls {@code .get()}. Spinning of work in another thread is the opposite: it starts
     * work immediately in the background. To the caller, this promise implementation feels
     * similar: creating one returns immediately, and the caller can use it almost exactly as a
     * "normal" asynchronous promise or future. The only difference is that with this solution,
     * callbacks won't happen until you ask for the result with {@code .get()}. With a normal
     * thread-based, async promise the callbacks happen at some point in the future regardless of
     * if anything ever calls {@code .get()}.
     *
     * @param <U> The type of the result of the promise. See {@link Promise}.
     */
    static class LazyPromise<U> implements Promise<U> {
        /**
         * A function which should trigger the completion of this promise with a result. If it does
         * not, this is a runtime error.
         *
         * <p>This is what makes the promise <em>lazy</em>: we can use this to complete the promise
         * on demand.
         */
        private final Completer completer;

        private U result;
        private Exception exception;
        private boolean completed = false;
        private boolean cancelled = false;

        /**
         * Queued up promises which are the result of applying this promise's value to some
         * function aka {@link PromiseHandler}. Promises are queued up by calling
         * {@link #then(PromiseHandler)} and {@link #thenPromise(PromiseHandler)}.
         */
        private final List<HandlingPromise<U, ?>> next = new ArrayList<>(1);

        /**
         * @param completer Reference to a function which should complete this promise when called.
         *                  See {@link #completer}.
         */
        LazyPromise(Completer completer) {
            this.completer = completer;
        }

        void complete(U responses) {
            if (isDone()) return;

            try {
                result = responses;
                completed = true;
                for (HandlingPromise<U, ?> next : this.next) {
                    next.complete(result);
                }
            } catch (Exception e) {
                completeExceptionally(e);
            }
        }

        void completeExceptionally(Exception exception) {
            if (isDone()) return;
            completed = true;
            this.exception = exception;
            for (HandlingPromise<U, ?> next : this.next) {
                next.completeExceptionally(exception);
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (completed) return false;
            cancelled = true;
            return true;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isDone() {
            return cancelled || completed;
        }

        @Override
        public U get() throws InterruptedException, ExecutionException {
            if (cancelled) {
                throw new CancellationException();
            }

            if (!completed) {
                completer.triggerPromiseCompletion();

                if (!completed) {
                    throw new ExecutionException(new IllegalStateException("Promise attempted to " +
                            "lazily trigger completion, but completer did not actually complete " +
                            "the promise. Check the provided completer function for correctness."));
                }
            }

            if (exception != null) {
                throw new ExecutionException(exception);
            }

            return result;
        }

        // TODO(ahenning): This ignores the timeout because we aren't doing work in another thread
        // I'm not sure that anyone will ever care, but if they did, we would do the same lazy
        // stuff, but just do it in another thread which we could interrupt if it took too long.
        // In that case we may have to build in interrupt checking during request processing.
        @Override
        public U get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
                TimeoutException {
            return get();
        }

        @Override
        public <V> Promise<V> then(PromiseHandler<U, V> promiseHandler) {
            HandlingPromise<U, V> promise = new HandlingPromise<>(promiseHandler, completer);
            next.add(promise);
            return promise;
        }

        @Override
        public <V> Promise<V> thenPromise(PromiseHandler<U, Promise<V>> promiseHandler) {
            HandlingPromise<U, Promise<V>> promise = new HandlingPromise<>(promiseHandler, completer);
            next.add(promise);
            return new PromiseOfPromise<>(promise);
        }
    }

    /**
     * Wraps a {@link LazyPromise} and some {@link AbstractLightblueDataRequest lightblue requests}
     * which are used to complete this in {@link #doQueuedRequestsAndCompleteFutures()}. Naturally,
     * then, that function is used as the lazy promise's completer function. That function and this
     * implementation are tightly coupled.
     */
    class LazyRequestPromise implements Promise<LightblueResponses> {
        private final LazyPromise<LightblueResponses> backingPromise =
                new LazyPromise<>(() -> doQueuedRequestsAndCompleteFutures());

        final AbstractLightblueDataRequest[] requests;

        LazyRequestPromise(AbstractLightblueDataRequest[] requests) {
            this.requests = requests;
        }

        void complete(LightblueResponses responses) {
            backingPromise.complete(responses);
        }

        void completeExceptionally(Exception exception) {
            backingPromise.completeExceptionally(exception);
        }

        @Override
        public <U> Promise<U> then(PromiseHandler<LightblueResponses, U> promiseHandler) {
            return backingPromise.then(promiseHandler);
        }

        @Override
        public <U> Promise<U> thenPromise(PromiseHandler<LightblueResponses, Promise<U>> promiseHandler) {
            return backingPromise.thenPromise(promiseHandler);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return backingPromise.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return backingPromise.isCancelled();
        }

        @Override
        public boolean isDone() {
            return backingPromise.isDone();
        }

        @Override
        public LightblueResponses get() throws InterruptedException, ExecutionException {
            return backingPromise.get();
        }

        @Override
        public LightblueResponses get(long timeout, TimeUnit unit) throws InterruptedException,
                ExecutionException, TimeoutException {
            return backingPromise.get(timeout, unit);
        }
    }

    /**
     * A simple lazy promise implementation which applies some transformation function to the value
     * it is completed with.
     *
     * <p>This is used in {@link #then(PromiseHandler)} callbacks of promise implementations.
     *
     * @param <T> The input type used to complete the promise.
     * @param <U> The output type as a result of applying the handler function to the input.
     */
    static class HandlingPromise<T, U> implements Promise<U> {
        private final PromiseHandler<T, U> handler;
        private final LazyPromise<U> backingPromise;

        HandlingPromise(PromiseHandler<T, U> handler, Completer completer) {
            this.handler = handler;
            this.backingPromise = new LazyPromise<>(completer);
        }

        void complete(T responses) {
            if (isDone()) return;

            try {
                U handled = handler.handle(responses);
                backingPromise.complete(handled);
            } catch (Exception e) {
                completeExceptionally(e);
            }
        }

        void completeExceptionally(Exception exception) {
            backingPromise.completeExceptionally(exception);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return backingPromise.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return backingPromise.isCancelled();
        }

        @Override
        public boolean isDone() {
            return backingPromise.isDone();
        }

        @Override
        public U get() throws InterruptedException, ExecutionException {
            return backingPromise.get();
        }

        /**
         * This future is not completed asynchronously so there is no way to "time out" unless we
         * introduce another thread for processing requests which I don't think is hugely necessary.
         */
        @Override
        public U get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
                TimeoutException {
            return backingPromise.get(timeout, unit);
        }

        @Override
        public <V> Promise<V> then(PromiseHandler<U, V> promiseHandler) {
            return backingPromise.then(promiseHandler);
        }

        @Override
        public <V> Promise<V> thenPromise(PromiseHandler<U, Promise<V>> promiseHandler) {
            return backingPromise.thenPromise(promiseHandler);
        }
    }

    interface Completer {
        void triggerPromiseCompletion();
    }
}
