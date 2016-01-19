package org.esbtools.eventhandler.lightblue;

import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.Locking;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class AutoLocker implements LockStrategy {
    private final Locking locking;
    private final Duration tryAcquireInterval;

    public AutoLocker(Locking locking, Duration tryAcquireInterval) {
        this.locking = locking;
        this.tryAcquireInterval = tryAcquireInterval;
    }

    public LightblueLock blockUntilAcquiredPingingEvery(Duration autoPingInterval,
            String... resourceIds) throws InterruptedException {
        while (true) {
            try {
                return new AggregateAutoPingingLock(locking.getCallerId(), resourceIds, locking,
                        autoPingInterval);
            } catch (Exception e) {
                Thread.sleep(tryAcquireInterval.toMillis());
            }
        }
    }

    static final class AutoPingingLock implements LightblueLock {
        private final String callerId;
        private final String resourceId;
        private final Locking locking;
        private final ScheduledExecutorService autoPingScheduler =
                Executors.newSingleThreadScheduledExecutor();
        private final ScheduledFuture<?> autoPinger;

        private static final Logger logger = LoggerFactory.getLogger(AutoPingingLock.class);

        public AutoPingingLock(Locking locking, String callerId, String resourceId,
                Duration autoPingInterval, Duration ttl) throws LightblueException,
                LockNotAvailableException {
            this.callerId = callerId;
            this.resourceId = resourceId;
            this.locking = locking;

            if (!locking.acquire(callerId, resourceId, ttl.toMillis())) {
                throw new LockNotAvailableException(callerId, resourceId);
            }

            this.autoPinger = autoPingScheduler.scheduleWithFixedDelay(() -> {
                try {
                    if (!locking.ping(callerId, resourceId)) {
                        throw new RuntimeException(
                                new LostLockException(this, "Lost lock. Will stop pinging."));
                    }
                } catch (LightblueException e) {
                    logger.error("Periodic lock ping failed for callerId <{}> and resourceId <{}>",
                            callerId, resourceId, e);
                }
            }, autoPingInterval.toMillis(), autoPingInterval.toMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public boolean ping() throws LightblueException {
            return locking.ping(callerId, resourceId);
        }

        @Override
        public void close() throws IOException {
            try {
                autoPinger.cancel(true);
                autoPingScheduler.shutdownNow();
                locking.release(callerId, resourceId);
            } catch (LightblueException e) {
                throw new IOException("Unable to release lock. callerId: " + callerId +
                        ", resourceId: " + resourceId, e);
            }
        }
    }

    static final class AggregateAutoPingingLock implements LightblueLock {
        private final List<AutoPingingLock> locks;

        private static final Logger logger = LoggerFactory.getLogger(AggregateAutoPingingLock.class);

        public AggregateAutoPingingLock(String callerId, String[] resourceIds, Locking locking,
                Duration autoPingInterval) throws LightblueException, LockNotAvailableException {
            locks = new ArrayList<>(resourceIds.length);

            Arrays.sort(resourceIds, null);

            // TODO: Parameterize ttl x autoping factor?
            Duration ttl = autoPingInterval.multipliedBy(5);

            try {
                for (String resourceId : resourceIds) {
                    locks.add(new AutoPingingLock(locking, callerId, resourceId,
                            autoPingInterval, ttl));
                }
            } catch (LockNotAvailableException e) {
                try {
                    close();
                } catch (IOException io) {
                    throw new LightblueException("Caught IOException trying to release locks. " +
                            "Unreleased locks should expire in " + ttl, io);
                }

                throw e;
            }
        }

        @Override
        public boolean ping() {
            boolean allLocksStillValid = true;

            for (LightblueLock lock : locks) {
                try {
                    if (!lock.ping()) {
                        allLocksStillValid = false;
                    }
                } catch (LightblueException e) {
                    logger.error("Failed to ping lock. Pessimistically assuming invalid. " +
                            "Lock was: {}", lock, e);
                    allLocksStillValid = false;
                }
            }

            return allLocksStillValid;
        }

        @Override
        public void close() throws IOException {
            List<IOException> exceptions = new ArrayList<>(0);

            for (LightblueLock lock : locks) {
                try {
                    lock.close();
                } catch (IOException e) {
                    exceptions.add(e);
                }
            }

            if (!exceptions.isEmpty()) {
                if (exceptions.size() == 1) {
                    throw exceptions.get(0);
                }

                throw new MultipleIOExceptions(exceptions);
            }
        }

        final static class MultipleIOExceptions extends IOException {
            MultipleIOExceptions(List<IOException> exceptions) {
                super("Multiple IOExceptions occurred. See suppressed exceptions.");

                exceptions.forEach(this::addSuppressed);
            }
        }
    }
}
