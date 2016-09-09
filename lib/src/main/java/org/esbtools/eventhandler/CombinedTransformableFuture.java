package org.esbtools.eventhandler;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class CombinedTransformableFuture<T> implements TransformableFuture<List<T>> {
    private final List<TransformableFuture<? extends T>> futures;
    private final List<FutureDoneCallback> doneCallbacks = new ArrayList<>();

    public CombinedTransformableFuture(TransformableFuture<? extends T>... futures) {
        this.futures = Arrays.asList(futures);
        initDoneCallbacks();
    }

    public CombinedTransformableFuture(List<TransformableFuture<? extends T>> futures) {
        this.futures = new ArrayList<>(futures);
        initDoneCallbacks();
    }

    private void initDoneCallbacks() {
        AtomicInteger doneCount = new AtomicInteger(0);
        this.futures.forEach(f -> f.whenDoneOrCancelled(() -> {
            if (doneCount.incrementAndGet() == this.futures.size()) {
                doneCallbacks.forEach(this::doCallback);
            }
        }));
    }

    @Override
    public <U> TransformableFuture<U> transformSync(FutureTransform<List<T>, U> futureTransform) {
        return new CombinedTransformingFuture<>(this, futureTransform);
    }

    @Override
    public <U> TransformableFuture<U> transformAsync(FutureTransform<List<T>, TransformableFuture<U>> futureTransform) {
        return new NestedTransformableFuture<>(
                new CombinedTransformingFuture<>(this, futureTransform));
    }

    @Override
    public TransformableFuture<Void> transformAsyncIgnoringReturn(FutureTransform<List<T>, TransformableFuture<?>> futureTransform) {
        return new NestedTransformableFutureIgnoringReturn(
                new CombinedTransformingFuture<>(this, futureTransform));
    }

    @Override
    public TransformableFuture<List<T>> whenDoneOrCancelled(FutureDoneCallback callback) {
        // TODO: there is a race condition here
        if (isDone()) {
            doCallback(callback);
        } else {
            doneCallbacks.add(callback);
        }

        return this;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean allCanceled = true;

        for (Future<?> future : futures) {
            if (!future.cancel(mayInterruptIfRunning)) {
                allCanceled = false;
            }
        }

        return allCanceled;
    }

    @Override
    public boolean isCancelled() {
        for (Future<?> future : futures) {
            if (future.isCancelled()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean isDone() {
        for (Future<?> future : futures) {
            if (!future.isDone()) {
                return false;
            }
        }

        return true;
    }

    @Override
    public List<T> get() throws InterruptedException, ExecutionException {
        @Nullable ExecutionException exception = null;
        @Nullable InterruptedException interruption = null;
        List<T> result = new ArrayList<>(futures.size());

        for (Future<? extends T> future : futures) {
            try {
                result.add(future.get());
            } catch (InterruptedException e) {
                if (interruption == null) {
                    interruption = new InterruptedException();
                }

                interruption.addSuppressed(e);
            } catch (ExecutionException e) {
                if (exception == null) {
                    exception = e;
                } else {
                    exception.addSuppressed(e);
                }
            } catch (RuntimeException e) {
                // Yes, a little odd to catch RuntimeException, but we need to make sure we've
                // examined all futures before returning.
                if (exception == null) {
                    exception = new ExecutionException(e);
                } else {
                    exception.addSuppressed(e);
                }
            }
        }

        if (exception != null) {
            addSuppressed(exception, interruption);
            throw exception;
        }

        if (interruption != null) {
            throw interruption;
        }

        return result;
    }

    @Override
    public List<T> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
        @Nullable ExecutionException exception = null;
        @Nullable InterruptedException interruption = null;
        @Nullable TimeoutException timeoutException = null;
        List<T> result = new ArrayList<>(futures.size());

        for (Future<? extends T> future : futures) {
            try {
                // TODO: Technically we should take time left between calls
                result.add(future.get(timeout, unit));
            } catch (TimeoutException e) {
                if (timeoutException == null) {
                    timeoutException = e;
                } else {
                    timeoutException.addSuppressed(e);
                }
            } catch (InterruptedException e) {
                if (interruption == null) {
                    interruption = e;
                } else {
                    interruption.addSuppressed(e);
                }
            } catch (ExecutionException e) {
                if (exception == null) {
                    exception = e;
                } else {
                    exception.addSuppressed(e);
                }
            } catch (RuntimeException e) {
                // Yes, a little odd to catch RuntimeException, but we need to make sure we've
                // examined all futures before returning.
                if (exception == null) {
                    exception = new ExecutionException(e);
                } else {
                    exception.addSuppressed(e);
                }
            }
        }

        if (exception != null) {
            addSuppressed(exception, interruption);
            addSuppressed(exception, timeoutException);
            throw exception;
        }

        if (interruption != null) {
            addSuppressed(interruption, timeoutException);
            throw interruption;
        }

        if (timeoutException != null) {
            throw timeoutException;
        }

        return result;
    }

    private void doCallback(FutureDoneCallback futureDoneCallback) {
        try {
            futureDoneCallback.onDoneOrCancelled();
        } catch (Exception e) {
            // TODO log
        }
    }

    private static void addSuppressed(@Nonnull Exception exception, @Nullable Exception toSuppress) {
        if (toSuppress != null && exception != toSuppress) {
            exception.addSuppressed(toSuppress);
        }
    }
}
