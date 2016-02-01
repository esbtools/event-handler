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

import org.esbtools.eventhandler.lightblue.LockNotAvailableException;
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
import java.util.stream.Collectors;

public class InMemoryLockStrategy implements LockStrategy {
    private static final Map<String, String> resourcesToClients =
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

            return new InMemoryLockedResources<>(resourceIds);
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

            String otherClientOrNull = resourcesToClients.putIfAbsent(resource.toString(), clientId);

            if (otherClientOrNull != null) {
                continue;
            }

            acquired.add(resource);
        }

        acquiredResources.addAll(acquired);

        if (allowLockButImmediateLoseIt) {
            releaseAll();
        }

        return new InMemoryLockedResources<>(acquired);
    }

    @Override
    public <T> LockedResource<T> tryAcquire(T resource) throws LockNotAvailableException {
        String otherClientOrNull = resourcesToClients.putIfAbsent(resource.toString(), clientId);

        if (otherClientOrNull == null) {
            acquiredResources.add(resource);

            if (allowLockButImmediateLoseIt) {
                releaseAll();
            }

            return new InMemoryLockedResource<T>(resource);
        }

        throw new LockNotAvailableException(resource);
    }

    public void releaseAll() {
        List<Object> currentlyAcquired = new ArrayList<>(acquiredResources);
        currentlyAcquired.forEach(r -> resourcesToClients.remove(r.toString()));
        acquiredResources.removeAll(currentlyAcquired);

    }

    private <T> void acquireAll(T[] resources) throws InterruptedException {
        for (T resourceId : resources) {
            String otherClientOrNull = resourcesToClients.putIfAbsent(resourceId.toString(), clientId);

            if (otherClientOrNull != null) {
                throw new IllegalStateException();
            }

            acquiredResources.add(resourceId);
        }
    }

    public void allowLockButImmediateLoseIt() {
        allowLockButImmediateLoseIt = true;
    }

    private class InMemoryLockedResource<T> implements LockedResource<T> {
        private final T resource;

        private InMemoryLockedResource(T resource) {
            this.resource = resource;
        }

        @Override
        public void ensureAcquiredOrThrow(String lostLockMessage) throws LostLockException {
            if (!acquiredResources.contains(resource)) {
                throw new LostLockException(this, lostLockMessage);
            }
        }

        @Override
        public T getResource() {
            return resource;
        }

        @Override
        public void close() throws IOException {
            resourcesToClients.remove(resource.toString());
            acquiredResources.remove(resource);
        }
    }

    private class InMemoryLockedResources<T> implements LockedResource<List<T>>, LockedResources<T> {
        private final List<T> resources;

        @SafeVarargs
        public InMemoryLockedResources(T... resources) {
            this.resources = Arrays.asList(resources);
        }

        public InMemoryLockedResources(List<T> resources) {
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
            resources.forEach(r -> resourcesToClients.remove(r.toString()));
            acquiredResources.removeAll(resources);
        }

        @Override
        public List<T> getResources() {
            return getResource();
        }

        @Override
        public List<LockedResource<T>> getLocks() {
            return resources.stream().map(InMemoryLockedResource::new).collect(Collectors.toList());
        }
    }
}
