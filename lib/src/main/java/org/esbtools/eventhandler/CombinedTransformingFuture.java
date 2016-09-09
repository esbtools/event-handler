package org.esbtools.eventhandler;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CombinedTransformingFuture<T, U> implements TransformableFuture<U> {
    private final CombinedTransformableFuture<T> combined;
    private final FutureTransform<List<T>, U> futureTransform;

    public CombinedTransformingFuture(CombinedTransformableFuture<T> combined,
            FutureTransform<List<T>, U> futureTransform) {
        this.combined = Objects.requireNonNull(combined, "combined");
        this.futureTransform = Objects.requireNonNull(futureTransform, "futureTransform");
    }

    @Override
    public <V> TransformableFuture<V> transformSync(FutureTransform<U, V> futureTransform) {
        return new CombinedTransformingFuture<>(combined,
                input -> futureTransform.transform(this.futureTransform.transform(input)));
    }

    @Override
    public <V> TransformableFuture<V> transformAsync(FutureTransform<U,
            TransformableFuture<V>> futureTransform) {
        return new NestedTransformableFuture<>(
                new CombinedTransformingFuture<>(combined,
                        input -> futureTransform.transform(this.futureTransform.transform(input))));
    }

    @Override
    public TransformableFuture<Void> transformAsyncIgnoringReturn(
            FutureTransform<U, TransformableFuture<?>> futureTransform) {
        return new NestedTransformableFutureIgnoringReturn(
                new CombinedTransformingFuture<>(combined,
                        input -> futureTransform.transform(this.futureTransform.transform(input))));
    }

    @Override
    public TransformableFuture<U> whenDoneOrCancelled(FutureDoneCallback callback) {
        combined.whenDoneOrCancelled(callback);
        return this;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return combined.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return combined.isCancelled();
    }

    @Override
    public boolean isDone() {
        return combined.isDone();
    }

    @Override
    public U get() throws InterruptedException, ExecutionException {
        try {
            return futureTransform.transform(combined.get());
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public U get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
        try {
            return futureTransform.transform(combined.get(timeout, unit));
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

}
