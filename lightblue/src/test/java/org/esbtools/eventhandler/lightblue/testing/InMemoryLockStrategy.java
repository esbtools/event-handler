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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class InMemoryLockStrategy implements LockStrategy {
    private static final Map<String, String> resourcesToClients =
            Collections.synchronizedMap(new HashMap<>());

    private final String clientId;
    private boolean allowLockButImmediateLoseIt = false;

    public InMemoryLockStrategy() {
        this(UUID.randomUUID().toString());
    }

    public InMemoryLockStrategy(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public <T> LockedResource<T> tryAcquire(String resourceId, T resource) throws LockNotAvailableException {
        String otherClientOrNull = resourcesToClients.putIfAbsent(resourceId, clientId);

        if (otherClientOrNull == null) {
            if (allowLockButImmediateLoseIt) {
                releaseAll();
            }

            return new InMemoryLockedResource<T>(resourceId, resource);
        }

        throw new LockNotAvailableException(resourceId, resource);
    }

    public void releaseAll() {
        synchronized (resourcesToClients) {
            Iterator<Map.Entry<String, String>> entries = resourcesToClients.entrySet().iterator();
            while (entries.hasNext()) {
                Map.Entry<String, String> entry = entries.next();
                if (entry.getValue().equals(clientId)) {
                    entries.remove();
                }
            }
        }
    }

    public void allowLockButImmediateLoseIt() {
        allowLockButImmediateLoseIt = true;
    }

    private class InMemoryLockedResource<T> implements LockedResource<T> {
        private final String resourceId;
        private final T resource;

        private InMemoryLockedResource(String resourceId, T resource) {
            this.resourceId = resourceId;
            this.resource = resource;
        }

        @Override
        public void ensureAcquiredOrThrow(String lostLockMessage) throws LostLockException {
            if (!resourcesToClients.containsKey(resourceId)) {
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
