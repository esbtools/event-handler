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

package org.esbtools.eventhandler.lightblue.locking;

import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.Locking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Uses lightblue's locking APIs with TTL, automatically pinging the lock in the background until it
 * is released.
 *
 * <p>Lightblue locks without TTLs are unsafe. If a client crashes, the lock will not be released.
 * However, locks with TTLs may expire prematurely. By pinging the lock in a separate thread
 * periodically, we can substantially lesson the likelihood of unintentionally losing the lock.
 *
 * <p>Consumers are expected to check the lock at critical points to ensure it did not expire by
 * calling {@link LockedResource#ensureAcquiredOrThrow(String)}.
 */
public class LightblueAutoPingLockStrategy implements LockStrategy {
    private final Locking locking;
    private final Duration autoPingInterval;
    private final Duration timeToLive;

    /**
     * Same as {@link #LightblueAutoPingLockStrategy(Locking, Duration, Duration)}
     * except the {@code timeToLive} duration defaults to 5 times the {@code autoPingInterval}.
     */
    public LightblueAutoPingLockStrategy(Locking locking, Duration autoPingInterval) {
        this(locking, autoPingInterval, autoPingInterval.multipliedBy(5));
    }

    /**
     * Validates {@code timeToLive} is greater than the {@code autoPingInterval}.
     * @param locking The locking client to use which holds knowledge of the locking domain to use.
     *                The default clientId is ignored.
     * @param autoPingInterval Amount of time in between automatic pings of acquired locks.
     * @param timeToLive Time until locks automatically expire. Should be [much] larger than the
     *                   {@code autoPingInterval} to ensure locks do not accidentally expire.
     */
    public LightblueAutoPingLockStrategy(Locking locking, Duration autoPingInterval,
            Duration timeToLive) {
        this.locking = locking;
        this.autoPingInterval = autoPingInterval;
        this.timeToLive = timeToLive;

        if (timeToLive.compareTo(autoPingInterval) <= 0) {
            throw new IllegalArgumentException("Time to live should be greater than auto ping " +
                    "interval, otherwise the lock will likely be lost.");
        }
    }

    @Override
    public <T> LockedResource<T> tryAcquire(String resourceId, T resource) throws LockNotAvailableException {
        try {
            // TODO: May want to include hostname and/or thread information in clientId
            String callerId = UUID.randomUUID().toString();
            return new AutoPingingLock<>(locking, callerId, resourceId, resource, autoPingInterval,
                    timeToLive);
        } catch (LightblueException e) {
            throw new LockNotAvailableException(resourceId, resource, e);
        }
    }

    static final class AutoPingingLock<T> implements LockedResource<T> {
        private final String callerId;
        private final T resource;
        private final String resourceId;
        private final Locking locking;
        private final ScheduledExecutorService autoPingScheduler =
                Executors.newSingleThreadScheduledExecutor();
        private final ScheduledFuture<?> autoPinger;

        private static final Logger logger = LoggerFactory.getLogger(AutoPingingLock.class);

        AutoPingingLock(Locking locking, String callerId, String resourceId, T resource,
                Duration autoPingInterval, Duration ttl) throws LightblueException,
                LockNotAvailableException {
            this.callerId = callerId;
            this.resource = resource;
            this.locking = locking;
            this.resourceId = resourceId;

            if (!locking.acquire(callerId, resourceId, ttl.toMillis())) {
                throw new LockNotAvailableException(resourceId, resource);
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
                    autoPinger.cancel(true);
                    autoPingScheduler.shutdownNow();
                    throw new LostLockException(this, lostLockMessage);
                }
            } catch (LightblueException e) {
                throw new LostLockException(this, "Failed to ping lock, assuming lost. " +
                        lostLockMessage, e);
            }
        }

        @Override
        public T getResource() {
            return resource;
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
                        lock.autoPinger.cancel(true);
                        lock.autoPingScheduler.shutdownNow();
                        throw new RuntimeException("Lost lock. Will stop pinging. Lock was: " + lock);
                    }
                } catch (LightblueException e) {
                    logger.error("Periodic lock ping failed for callerId <{}> and resourceId <{}>." +
                            "Will keep trying.", lock.callerId, lock.resourceId, e);
                }
            }
        }
    }

}
