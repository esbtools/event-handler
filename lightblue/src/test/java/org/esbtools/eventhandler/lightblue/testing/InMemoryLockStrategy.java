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
import org.esbtools.eventhandler.lightblue.LostLockException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class InMemoryLockStrategy implements LockStrategy {
    private static final Map<String, String> resourcesToClients =
            Collections.synchronizedMap(new HashMap<>());

    private final String clientId;
    private final List<String> acquiredResources = new ArrayList<>();

    public InMemoryLockStrategy() {
        this(UUID.randomUUID().toString());
    }

    public InMemoryLockStrategy(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public LockedResource blockUntilAcquired(String... resourceIds) throws InterruptedException {
        while (true) {
            try {
                acquireAll(resourceIds);
            } catch (IllegalStateException e) {
                acquiredResources.forEach(resourcesToClients::remove);
                Thread.sleep(500);
                continue;
            }

            return new InMemoryLockedResource();
        }
    }

    private void acquireAll(String[] resourceIds) throws InterruptedException {
        for (String resourceId : resourceIds) {
            String otherClientOrNull = resourcesToClients.putIfAbsent(resourceId, clientId);

            if (otherClientOrNull != null) {
                throw new IllegalStateException();
            }

            acquiredResources.add(resourceId);
        }
    }

    private class InMemoryLockedResource implements LockedResource {
        @Override
        public void ping(String lostLockMessage) throws LostLockException {
            // Lock is never lost
        }

        @Override
        public void close() throws IOException {
            acquiredResources.forEach(resourcesToClients::remove);
        }
    }
}
