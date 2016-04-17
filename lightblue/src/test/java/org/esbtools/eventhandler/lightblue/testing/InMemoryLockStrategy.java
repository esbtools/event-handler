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

package org.esbtools.eventhandler.lightblue.testing;

import org.esbtools.eventhandler.lightblue.locking.LockNotAvailableException;
import org.esbtools.eventhandler.lightblue.locking.LockStrategy;
import org.esbtools.eventhandler.lightblue.locking.LockedResource;
import org.esbtools.eventhandler.lightblue.locking.LostLockException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class InMemoryLockStrategy implements LockStrategy {
    private static final Map<String, String> resourcesToClients =
            Collections.synchronizedMap(new HashMap<>());

    private boolean allowLockButImmediateLoseIt = false;
    private CountDownLatch pauseLatch = new CountDownLatch(0);
    private volatile CountDownLatch waitForLockLatch = new CountDownLatch(1);
    private volatile CountDownLatch waitForTryAcquireLatch = new CountDownLatch(1);

    public InMemoryLockStrategy() {
    }

    @Override
    public <T> LockedResource<T> tryAcquire(String resourceId, T resource) throws LockNotAvailableException {
        String clientId = UUID.randomUUID().toString();
        String otherClientOrNull = resourcesToClients.putIfAbsent(resourceId, clientId);

        waitForTryAcquireLatch.countDown();
        waitForTryAcquireLatch = new CountDownLatch(1);

        if (otherClientOrNull == null) {
            if (allowLockButImmediateLoseIt) {
                resourcesToClients.remove(resourceId);
            }

            LockedResource<T> lock = new InMemoryLockedResource<T>(clientId, resourceId, resource);

            waitForLockLatch.countDown();
            waitForLockLatch = new CountDownLatch(1);

            while (true) {
                try {
                    pauseLatch.await();
                    return lock;
                } catch (InterruptedException ignored) {
                    // Keep waiting...
                }
            }
        }

        throw new LockNotAvailableException(resourceId, resource);
    }

    public LockedResource<String> forceAcquire(String resourceId) {
        String clientId = UUID.randomUUID().toString();
        resourcesToClients.put(resourceId, clientId);
        return new InMemoryLockedResource<>(clientId, resourceId, resourceId);
    }

    public void releaseAll() {
        resourcesToClients.clear();
    }

    public void waitForTryAcquire() throws InterruptedException {
        waitForTryAcquireLatch.await();
    }

    public void waitForLock() throws InterruptedException {
        waitForLockLatch.await();
    }

    public void pauseAfterLock() {
        if (pauseLatch.getCount() > 0) {
            return;
        }

        pauseLatch = new CountDownLatch(1);
    }

    public void unpause() {
        pauseLatch.countDown();
    }

    public void allowLockButImmediateLoseIt() {
        allowLockButImmediateLoseIt = true;
    }

    private class InMemoryLockedResource<T> implements LockedResource<T> {
        private final String clientId;
        private final String resourceId;
        private final T resource;

        private InMemoryLockedResource(String clientId, String resourceId, T resource) {
            this.clientId = clientId;
            this.resourceId = resourceId;
            this.resource = resource;
        }

        @Override
        public void ensureAcquiredOrThrow(String lostLockMessage) throws LostLockException {
            if (!clientId.equals(resourcesToClients.get(resourceId))) {
                throw new LostLockException(this, lostLockMessage);
            }
        }

        public T getResource() {
            return resource;
        }

        @Override
        public void close() {
            resourcesToClients.remove(resourceId);
        }

        @Override
        public String toString() {
            return "InMemoryLockedResource{" +
                    "resourceId='" + resourceId + '\'' +
                    ", resource=" + resource +
                    '}';
        }
    }
}
