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

import org.esbtools.eventhandler.lightblue.LockStrategy;
import org.esbtools.eventhandler.lightblue.LockedResource;
import org.esbtools.eventhandler.lightblue.LockedResources;
import org.esbtools.eventhandler.lightblue.LostLockException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class InMemoryLockStrategy implements LockStrategy {
    private static final Map<Object, String> resourcesToClients =
            Collections.synchronizedMap(new HashMap<>());

    private final String clientId;
    private final List<Object> acquiredResources = Collections.synchronizedList(new ArrayList<>());
    private boolean allowLockButImmediateLoseIt = false;

    public InMemoryLockStrategy() {
        this(UUID.randomUUID().toString());
    }

    public InMemoryLockStrategy(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public LockedResource<List<String>> blockUntilAcquired(String... resourceIds) throws InterruptedException {
        while (true) {
            try {
                acquireAll(resourceIds);
            } catch (IllegalStateException e) {
                releaseAll();
                Thread.sleep(500);
                continue;
            }

            if (allowLockButImmediateLoseIt) {
                releaseAll();
            }

            return new InMemoryLockedResource<>(resourceIds);
        }
    }

    @Override
    public <T> LockedResources<T> tryAcquireUpTo(int maxResources, Collection<T> resources) {
        List<T> acquired = new ArrayList<>(
                resources.size() > maxResources ? maxResources : resources.size());

        for (T resource : resources) {
            if (acquired.size() + 1 > maxResources) {
                break;
            }

            String otherClientOrNull = resourcesToClients.putIfAbsent(resource, clientId);

            if (otherClientOrNull != null) {
                continue;
            }

            acquired.add(resource);
        }

        acquiredResources.addAll(acquired);

        if (allowLockButImmediateLoseIt) {
            releaseAll();
        }

        return new InMemoryLockedResource<>(acquired);
    }

    public void releaseAll() {
        List<Object> currentlyAcquired = new ArrayList<>(acquiredResources);
        currentlyAcquired.forEach(resourcesToClients::remove);
        acquiredResources.removeAll(currentlyAcquired);

    }

    private <T> void acquireAll(T[] resources) throws InterruptedException {
        for (T resourceId : resources) {
            String otherClientOrNull = resourcesToClients.putIfAbsent(resourceId, clientId);

            if (otherClientOrNull != null) {
                throw new IllegalStateException();
            }

            acquiredResources.add(resourceId);
        }
    }

    public void allowLockButImmediateLoseIt() {
        allowLockButImmediateLoseIt = true;
    }

    private class InMemoryLockedResource<T> implements LockedResource<List<T>>, LockedResources<T> {
        private final List<T> resources;

        @SafeVarargs
        public InMemoryLockedResource(T... resources) {
            this.resources = Arrays.asList(resources);
        }

        public InMemoryLockedResource(List<T> resources) {
            this.resources = resources;
        }

        @Override
        public void ensureAcquiredOrThrow(String lostLockMessage) throws LostLockException {
            if (!acquiredResources.containsAll(resources)) {
                throw new LostLockException(this, lostLockMessage);
            }
        }

        @Override
        public List<T> getResource() {
            return Collections.unmodifiableList(resources);
        }

        @Override
        public void close() throws IOException {
            resources.forEach(resourcesToClients::remove);
            acquiredResources.removeAll(resources);
        }

        @Override
        public List<T> getResources() {
            return getResource();
        }

        @Override
        public List<T> checkForLostResources() {
            List<T> lost = new ArrayList<>(resources);
            lost.removeAll(acquiredResources);
            return lost;
        }
    }
}
