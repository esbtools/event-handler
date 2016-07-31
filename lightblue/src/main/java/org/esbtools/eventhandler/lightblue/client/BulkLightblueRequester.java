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

import org.esbtools.eventhandler.FutureDoneCallback;
import org.esbtools.eventhandler.FutureTransform;
import org.esbtools.eventhandler.NestedTransformableFuture;
import org.esbtools.eventhandler.NestedTransformableFutureIgnoringReturn;
import org.esbtools.eventhandler.Responses;
import org.esbtools.eventhandler.TransformableFuture;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

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
    private final List<LazyRequestTransformableFuture<LightblueDataResponses>> queuedRequests =
            Collections.synchronizedList(new ArrayList<>());
    private final List<LazyRequestTransformableFuture<LightblueResponses>> queuedTryRequests =
            Collections.synchronizedList(new ArrayList<>());

    public BulkLightblueRequester(LightblueClient lightblue) {
        this.lightblue = lightblue;
    }

    @Override
    public TransformableFuture<LightblueDataResponses> request(AbstractLightblueDataRequest... requests) {
        checkNoNullsInRequests(requests);
        LazyRequestTransformableFuture<LightblueDataResponses> responseFuture =
                new LazyRequestTransformableFuture<>(requests);
        queuedRequests.add(responseFuture);
        return responseFuture;
    }

    @Override
    public TransformableFuture<LightblueDataResponses> request(
            Collection<? extends AbstractLightblueDataRequest> requests) {
        return request(requests.toArray(new AbstractLightblueDataRequest[requests.size()]));
    }

    @Override
    public TransformableFuture<LightblueResponses> tryRequest(AbstractLightblueDataRequest... req) {
        checkNoNullsInRequests(req);
        LazyRequestTransformableFuture<LightblueResponses> responseFuture =
                new LazyRequestTransformableFuture<>(req);
        queuedTryRequests.add(responseFuture);
        return responseFuture;
    }

    private void doQueuedRequestsAndCompleteFutures() {
        List<LazyRequestTransformableFuture<LightblueDataResponses>> batch;
        List<LazyRequestTransformableFuture<LightblueResponses>> tryBatch;

        synchronized (queuedRequests) {
            batch = new ArrayList<>(queuedRequests);
            queuedRequests.clear();
        }

        synchronized (queuedTryRequests) {
            tryBatch = new ArrayList<>(queuedTryRequests);
            queuedTryRequests.clear();
        }

        DataBulkRequest bulkRequest = new DataBulkRequest();

        Stream.concat(batch.stream(), tryBatch.stream())
                .flatMap(requestFuture -> Arrays.stream(requestFuture.requests))
                // TODO: Determine if any requests are equivalent / duplicated and filter out
                .forEach(bulkRequest::add);

        try {
            LightblueBulkDataResponse bulkResponse = tryBulkRequest(bulkRequest);

            for (LazyRequestTransformableFuture<LightblueDataResponses> batchedFuture : batch) {
                AbstractLightblueDataRequest[] requests = batchedFuture.requests;
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
                    batchedFuture.complete(new BulkDataResponses(responseMap));
                } else {
                    batchedFuture.completeExceptionally(new LightblueResponseException(errors));
                }
            }

            for (LazyRequestTransformableFuture<LightblueResponses> batchedFuture : tryBatch) {
                AbstractLightblueDataRequest[] requests = batchedFuture.requests;
                Map<AbstractLightblueDataRequest, LightblueResponse> responseMap =
                        new HashMap<>(requests.length);

                for (AbstractLightblueDataRequest request : requests) {
                    LightblueDataResponse response = bulkResponse.getResponse(request);
                    responseMap.put(request, LightblueResponse.fromClientResponse(response));
                }

                batchedFuture.complete(new BulkResponses(responseMap));
            }
        } catch (Exception e) {
            Stream.concat(batch.stream(), tryBatch.stream())
                    .forEach(batchedFuture -> batchedFuture.completeExceptionally(e));
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

    private static void checkNoNullsInRequests(AbstractLightblueDataRequest[] requests) {
        Objects.requireNonNull(requests, "requests");
        for (int i = 0; i < requests.length; i++) {
            Objects.requireNonNull(requests[i], "requests[" + i + "]");
        }
    }

    /**
     * We have two response maps: one with guaranteed successful responses (
     * {@link BulkDataResponses} and one with no guarantees ({@link BulkResponses}). They are both
     * backed by simple maps, just with different generic types, hence the base class.
     */
    static abstract class ResponseMap<T> implements Responses<AbstractLightblueDataRequest, T> {
        private final Map<AbstractLightblueDataRequest, T> responseMap;

        ResponseMap(Map<AbstractLightblueDataRequest, T> responseMap) {
            this.responseMap = responseMap;
        }

        @Override
        public T forRequest(AbstractLightblueDataRequest request) {
            if (responseMap.containsKey(request)) {
                return responseMap.get(request);
            }

            throw new NoSuchElementException("No response for request: " + request);
        }
    }

    static class BulkDataResponses extends ResponseMap<LightblueDataResponse> implements LightblueDataResponses {
        BulkDataResponses(Map<AbstractLightblueDataRequest, LightblueDataResponse> responseMap) {
            super(responseMap);
        }
    }

    static class BulkResponses extends ResponseMap<LightblueResponse> implements LightblueResponses {
        BulkResponses(Map<AbstractLightblueDataRequest, LightblueResponse> responseMap) {
            super(responseMap);
        }
    }

    /**
     * A core future implementation which accepts its result externally, triggering this result
     * lazily on the first call to {@link #get()}.
     *
     * <p>This future is used to back more specific cases in {@link LazyRequestTransformableFuture}
     * and {@link LazyTransformingFuture}.
     *
     * <p>{@link Future}s typically work asynchronously in another thread. This implementation
     * intentionally does not. To reap the performance benefits of batching requests, we need a
     * future design which puts off doing <em>any</em> work as long as possible: until something
     * calls {@code .get()}. Spinning of work in another thread is the opposite: it starts work
     * immediately in the background. To the caller, this future implementation feels similar:
     * creating one returns immediately, and the caller can use it almost exactly as a "normal"
     * asynchronous future or future. The only difference is that with this solution, callbacks
     * won't happen until you ask for the result with {@code .get()}. With a normal thread-based,
     * async future the callbacks happen at some point in the future regardless of if anything ever
     * calls {@code .get()}.
     *
     * @param <U> The type of the result of the future. See {@link TransformableFuture}.
     */
    static class LazyTransformableFuture<U> implements TransformableFuture<U> {
        /**
         * A function which should trigger the completion of this future with a result. If it does
         * not, this is a runtime error.
         *
         * <p>This is what makes the future <em>lazy</em>: we can use this to complete the future
         * on demand.
         */
        private final Completer completer;

        private U result;
        private Exception exception;
        private boolean completed = false;
        private boolean cancelled = false;

        /**
         * Queued up futures which are the result of applying this future's value to some transform
         * function ({@link FutureTransform}). Futures are queued up by calling APIs like
         * {@link #transformSync(FutureTransform)} and {@link #transformAsync(FutureTransform)}.
         */
        private final List<LazyTransformingFuture<U, ?>> next = new ArrayList<>(1);

        private final List<FutureDoneCallback> doneCallbacks = new ArrayList<>();

        private static Logger log = LoggerFactory.getLogger(LazyTransformableFuture.class);

        /**
         * @param completer Reference to a function which should complete this future when called.
         *                  See {@link #completer}.
         */
        LazyTransformableFuture(Completer completer) {
            this.completer = completer;
        }

        void complete(U responses) {
            if (isDone()) return;

            result = responses;
            completed = true;
            callDoneCallbacks();

            for (LazyTransformingFuture<U, ?> next : this.next) {
                try {
                    next.complete(result);
                } catch (Exception e) {
                    log.warn("Exception caught and ignored while completing next transforming " +
                            "future.", e);
                }
            }
        }

        void completeExceptionally(Exception exception) {
            if (isDone()) return;
            completed = true;
            this.exception = exception;
            callDoneCallbacks();

            for (LazyTransformingFuture<U, ?> next : this.next) {
                try {
                    next.completeExceptionally(exception);
                } catch (Exception e) {
                    log.warn("Exception caught and ignored while completing next transforming " +
                            "future with exception.", e);
                }
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (completed) return false;
            cancelled = true;
            callDoneCallbacks();
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
                completer.triggerFutureCompletion();

                if (!completed) {
                    throw new ExecutionException(new IllegalStateException("Future attempted to " +
                            "lazily trigger completion, but completer did not actually complete " +
                            "the future. Check the provided completer function for correctness."));
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
        public <V> TransformableFuture<V> transformSync(FutureTransform<U, V> futureTransform) {
            LazyTransformingFuture<U, V> future = new LazyTransformingFuture<>(futureTransform, completer);
            next.add(future);
            return future;
        }

        @Override
        public <V> TransformableFuture<V> transformAsync(
                FutureTransform<U, TransformableFuture<V>> futureTransform) {
            LazyTransformingFuture<U, TransformableFuture<V>> future =
                    new LazyTransformingFuture<>(futureTransform, completer);
            next.add(future);
            return new NestedTransformableFuture<>(future);
        }

        @Override
        public TransformableFuture<Void> transformAsyncIgnoringReturn(
                FutureTransform<U, TransformableFuture<?>> futureTransform) {
            LazyTransformingFuture<U, TransformableFuture<?>> future =
                    new LazyTransformingFuture<>(futureTransform, completer);
            next.add(future);
            return new NestedTransformableFutureIgnoringReturn(future);
        }

        @Override
        public TransformableFuture<U> whenDoneOrCancelled(FutureDoneCallback callback) {
            if (isDone()) {
                try {
                    callback.onDoneOrCancelled();
                } catch (Exception e) {
                    log.warn("Exception caught and ignored while running future done callback.", e);
                }
                return this;
            }
            doneCallbacks.add(callback);
            return this;
        }

        private void callDoneCallbacks() {
            for (FutureDoneCallback doneCallback : doneCallbacks) {
                try {
                    doneCallback.onDoneOrCancelled();
                } catch (Exception e) {
                    log.warn("Exception caught and ignored while running future done callback.", e);
                }
            }
        }
    }

    /**
     * Wraps a {@link LazyTransformableFuture} and some {@link AbstractLightblueDataRequest
     * lightblue requests} which are used to complete this in
     * {@link #doQueuedRequestsAndCompleteFutures()}. Naturally, then, that function is used as the
     * lazy future's completer function. That function and this implementation are tightly coupled.
     */
    class LazyRequestTransformableFuture<T> implements TransformableFuture<T> {
        private final LazyTransformableFuture<T> backingFuture =
                new LazyTransformableFuture<>(() -> doQueuedRequestsAndCompleteFutures());

        final AbstractLightblueDataRequest[] requests;

        LazyRequestTransformableFuture(AbstractLightblueDataRequest[] requests) {
            this.requests = requests;
        }

        void complete(T responses) {
            backingFuture.complete(responses);
        }

        void completeExceptionally(Exception exception) {
            backingFuture.completeExceptionally(exception);
        }

        @Override
        public <U> TransformableFuture<U> transformSync(
                FutureTransform<T, U> futureTransform) {
            return backingFuture.transformSync(futureTransform);
        }

        @Override
        public <U> TransformableFuture<U> transformAsync(
                FutureTransform<T, TransformableFuture<U>> futureTransform) {
            return backingFuture.transformAsync(futureTransform);
        }

        @Override
        public TransformableFuture<Void> transformAsyncIgnoringReturn(
                FutureTransform<T, TransformableFuture<?>> futureTransform) {
            return backingFuture.transformAsyncIgnoringReturn(futureTransform);
        }

        @Override
        public TransformableFuture<T> whenDoneOrCancelled(FutureDoneCallback callback) {
            backingFuture.whenDoneOrCancelled(callback);
            return this;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (!backingFuture.isDone()) {
                queuedRequests.remove(this);
            }

            return backingFuture.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return backingFuture.isCancelled();
        }

        @Override
        public boolean isDone() {
            return backingFuture.isDone();
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            return backingFuture.get();
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException,
                ExecutionException, TimeoutException {
            return backingFuture.get(timeout, unit);
        }
    }

    /**
     * A simple lazy future implementation which applies some transformation function to the value
     * it is completed with.
     *
     * <p>This is used in {@link #transformSync(FutureTransform)} callbacks of
     * {@link TransformableFuture} implementations.
     *
     * @param <T> The input type used to complete the Future.
     * @param <U> The output type as a result of applying the transform function to the input.
     */
    static class LazyTransformingFuture<T, U> implements TransformableFuture<U> {
        private final FutureTransform<T, U> transform;
        private final LazyTransformableFuture<U> backingFuture;

        LazyTransformingFuture(FutureTransform<T, U> transform, Completer completer) {
            this.transform = transform;
            this.backingFuture = new LazyTransformableFuture<>(completer);
        }

        void complete(T responses) {
            if (isDone()) return;

            try {
                U transformed = transform.transform(responses);
                backingFuture.complete(transformed);
            } catch (Exception e) {
                completeExceptionally(e);
            }
        }

        void completeExceptionally(Exception exception) {
            backingFuture.completeExceptionally(exception);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return backingFuture.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return backingFuture.isCancelled();
        }

        @Override
        public boolean isDone() {
            return backingFuture.isDone();
        }

        @Override
        public U get() throws InterruptedException, ExecutionException {
            return backingFuture.get();
        }

        /**
         * This future is not completed asynchronously so there is no way to "time out" unless we
         * introduce another thread for processing requests which I don't think is hugely necessary.
         */
        @Override
        public U get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
                TimeoutException {
            return backingFuture.get(timeout, unit);
        }

        @Override
        public <V> TransformableFuture<V> transformSync(FutureTransform<U, V> futureTransform) {
            return backingFuture.transformSync(futureTransform);
        }

        @Override
        public <V> TransformableFuture<V> transformAsync(
                FutureTransform<U, TransformableFuture<V>> futureTransform) {
            return backingFuture.transformAsync(futureTransform);
        }

        @Override
        public TransformableFuture<Void> transformAsyncIgnoringReturn(
                FutureTransform<U, TransformableFuture<?>> futureTransform) {
            return backingFuture.transformAsyncIgnoringReturn(futureTransform);
        }

        @Override
        public TransformableFuture<U> whenDoneOrCancelled(FutureDoneCallback callback) {
            backingFuture.whenDoneOrCancelled(callback);
            return this;
        }
    }

    interface Completer {
        void triggerFutureCompletion();
    }
}
