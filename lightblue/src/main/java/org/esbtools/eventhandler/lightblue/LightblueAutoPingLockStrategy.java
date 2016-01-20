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

public class LightblueAutoPingLockStrategy implements LockStrategy {
    private final Locking locking;
    private final Duration tryAcquireInterval;
    private final Duration autoPingInterval;

    public LightblueAutoPingLockStrategy(Locking locking, Duration tryAcquireInterval,
            Duration autoPingInterval) {
        this.locking = locking;
        this.tryAcquireInterval = tryAcquireInterval;
        this.autoPingInterval = autoPingInterval;
    }

    @Override
    public LockedResource blockUntilAcquired(String... resourceIds) throws InterruptedException {
        while (true) {
            try {
                return new AggregateAutoPingingLock(locking.getCallerId(), resourceIds, locking,
                        autoPingInterval);
            } catch (Exception e) {
                Thread.sleep(tryAcquireInterval.toMillis());
            }
        }
    }

    static final class AutoPingingLock implements LockedResource {
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

            this.autoPinger = autoPingScheduler.scheduleWithFixedDelay(
                    new PingTask(this),
                    /* initial delay*/ autoPingInterval.toMillis(),
                    /* delay */ autoPingInterval.toMillis(),
                    TimeUnit.MILLISECONDS);
        }

        @Override
        public void ensureAcquiredOrThrow(String lostLockMessage) throws LostLockException {
            try {
                if (!locking.ping(callerId, resourceId)) {
                    throw new LostLockException(this, lostLockMessage);
                }
            } catch (LightblueException e) {
                throw new LostLockException(this, "Failed to ping lock, assuming lost. " +
                        lostLockMessage, e);
            }
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

        @Override
        public String toString() {
            return "AutoPingingLock{" +
                    "resourceId='" + resourceId + '\'' +
                    ", callerId='" + callerId + '\'' +
                    ", lockingDomain='" + locking.getDomain() + '\'' +
                    '}';
        }

        static class PingTask implements Runnable {
            final AutoPingingLock lock;

            PingTask(AutoPingingLock lock) {
                this.lock = lock;
            }

            @Override
            public void run() {
                try {
                    if (!lock.locking.ping(lock.callerId, lock.resourceId)) {
                        throw new RuntimeException("Lost lock. Will stop pinging. Lock was: " + lock);
                    }
                } catch (LightblueException e) {
                    logger.error("Periodic lock ping failed for callerId <{}> and resourceId <{}>",
                            lock.callerId, lock.resourceId, e);
                }
            }
        }
    }

    static final class AggregateAutoPingingLock implements LockedResource {
        private final List<AutoPingingLock> locks;

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
                    io.addSuppressed(e);

                    throw new LightblueException("Caught IOException trying to release locks. " +
                            "Unreleased locks should expire in " + ttl, io);
                }

                throw e;
            }
        }

        @Override
        public void ensureAcquiredOrThrow(String lostLockMessage) throws LostLockException {
            List<LostLockException> lostLockExceptions = new ArrayList<>(0);

            for (LockedResource lock : locks) {
                try {
                    lock.ensureAcquiredOrThrow(lostLockMessage);
                } catch (LostLockException e) {
                    lostLockExceptions.add(e);
                }
            }

            if (!lostLockExceptions.isEmpty()) {
                if (lostLockExceptions.size() == 1) {
                    throw lostLockExceptions.get(0);
                }

                throw new LostLockException(lostLockExceptions);
            }
        }

        @Override
        public void close() throws IOException {
            List<IOException> exceptions = new ArrayList<>(0);

            for (LockedResource lock : locks) {
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

        @Override
        public String toString() {
            return "AggregateAutoPingingLock{" +
                    "locks=" + locks +
                    '}';
        }

        final static class MultipleIOExceptions extends IOException {
            MultipleIOExceptions(List<IOException> exceptions) {
                super("Multiple IOExceptions occurred. See suppressed exceptions.");

                exceptions.forEach(this::addSuppressed);
            }
        }
    }

    static class LockNotAvailableException extends Exception {
        public LockNotAvailableException(String callerId, String resourceId) {
            super("callerId: " + callerId + ", resourceId: " + resourceId);
        }
    }
}
