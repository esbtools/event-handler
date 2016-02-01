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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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
    private final Duration tryAcquireInterval;
    private final Duration autoPingInterval;
    private final Duration timeToLive;

    /**
     * Same as {@link #LightblueAutoPingLockStrategy(Locking, Duration, Duration, Duration)}
     * except the {@code timeToLive} duration defaults to 5 times the {@code autoPingInterval}.
     */
    public LightblueAutoPingLockStrategy(Locking locking, Duration tryAcquireInterval,
            Duration autoPingInterval) {
        this(locking, tryAcquireInterval, autoPingInterval, autoPingInterval.multipliedBy(5));
    }

    /**
     * Validates {@code timeToLive} is greater than the {@code autoPingInterval}.
     * @param locking
     * @param tryAcquireInterval If the asked resources cannot be acquire, will wait this duration
     *                           before trying again.
     * @param autoPingInterval Amount of time in between automatic pings of acquired locks.
     * @param timeToLive Time until locks automatically expire. Should be [much] larger than the
     *                   {@code autoPingInterval} to ensure locks do not accidentally expire.
     */
    public LightblueAutoPingLockStrategy(Locking locking, Duration tryAcquireInterval,
            Duration autoPingInterval, Duration timeToLive) {
        this.locking = locking;
        this.tryAcquireInterval = tryAcquireInterval;
        this.autoPingInterval = autoPingInterval;
        this.timeToLive = timeToLive;

        if (timeToLive.compareTo(autoPingInterval) <= 0) {
            throw new IllegalArgumentException("Time to live should be greater than auto ping " +
                    "interval, otherwise the lock will likely be lost.");
        }
    }

    @Override
    public LockedResource blockUntilAcquired(String... resourceIds) throws InterruptedException {
        Objects.requireNonNull(resourceIds, "resourceIds");

        if (resourceIds.length == 0) {
            throw new IllegalArgumentException("Must provide one or more resourceIds.");
        }

        while (true) {
            try {
                String callerId = UUID.randomUUID().toString();
                return new AggregateAutoPingingLock(callerId, resourceIds, locking,
                        autoPingInterval, timeToLive);
            } catch (Exception e) {
                Thread.sleep(tryAcquireInterval.toMillis());
            }
        }
    }

    public <T> LockedResource<T> tryAcquire(T resource) throws LockNotAvailableException {
        try {
            String callerId = UUID.randomUUID().toString();
            return new AutoPingingLock<>(locking, callerId, resource, autoPingInterval, timeToLive);
        } catch (LightblueException e) {
            throw new LockNotAvailableException(resource.toString(), e);
        }
    }

    @Override
    public <T> LockedResources<T> tryAcquireUpTo(int maxResources, Collection<T> resources) {
        String callerId = UUID.randomUUID().toString();
        return new AutoPingingLocks<>(resources, maxResources, callerId, locking, timeToLive);
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

        AutoPingingLock(Locking locking, String callerId, T resource,
                Duration autoPingInterval, Duration ttl) throws LightblueException,
                LockNotAvailableException {
            this.callerId = callerId;
            this.resource = resource;
            this.locking = locking;

            try {
                resourceId = URLEncoder.encode(resource.toString(), "UTF-8");

                if (!locking.acquire(callerId, resourceId, ttl.toMillis())) {
                    throw new LockNotAvailableException(resourceId);
                }

                this.autoPinger = autoPingScheduler.scheduleWithFixedDelay(
                        new PingTask(this),
                        /* initial delay*/ autoPingInterval.toMillis(),
                        /* delay */ autoPingInterval.toMillis(),
                        TimeUnit.MILLISECONDS);
            } catch (UnsupportedEncodingException e) {
                // FIXME: What to do about this stupid exception...
                throw new RuntimeException(e);
            }
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
                        throw new RuntimeException("Lost lock. Will stop pinging. Lock was: " + lock);
                    }
                } catch (LightblueException e) {
                    logger.error("Periodic lock ping failed for callerId <{}> and resourceId <{}>",
                            lock.callerId, lock.resourceId, e);
                }
            }
        }
    }

    static final class AutoPingingLocks<T> implements LockedResources<T> {
        private final List<LockedResource<T>> locks;
        private final List<T> resourcesAcquired;

        static final Logger logger = LoggerFactory.getLogger(AutoPingingLocks.class);

        AutoPingingLocks(Collection<T> resources, int maxResources, String callerId,
                Locking locking, Duration ttl) {
            locks = new ArrayList<>(resources.size());
            resourcesAcquired = new ArrayList<>(resources.size());

            for (T resource : resources) {
                try {
                    locks.add(new AutoPingingLock<>(locking, callerId, resource, ttl.dividedBy(2),
                            ttl));
                    resourcesAcquired.add(resource);
                } catch (LightblueException e) {
                    logger.warn("LightblueException trying to acquire lock for resource: " +
                            resource + ", callerId: " + callerId, e);
                } catch (LockNotAvailableException ignored) {
                        // Fall through...
                }

                if (locks.size() == maxResources) {
                    break;
                }
            }
        }

        @Override
        public List<T> getResources() {
            return Collections.unmodifiableList(resourcesAcquired);
        }

        @Override
        public List<LockedResource<T>> getLocks() {
            return Collections.unmodifiableList(locks);
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
    }

    static final class AggregateAutoPingingLock implements LockedResource {
        private final List<AutoPingingLock> locks;

        AggregateAutoPingingLock(String callerId, String[] resourceIds, Locking locking,
                Duration autoPingInterval, Duration ttl) throws LightblueException, LockNotAvailableException {
            locks = new ArrayList<>(resourceIds.length);

            Arrays.sort(resourceIds, null);

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
        public Object getResource() {
            // FIXME
            return null;
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
    }

}
