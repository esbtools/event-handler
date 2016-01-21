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

import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.Locking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryLocking extends Locking {
    private static final Map<String, String> resourcesToCallers = Collections.synchronizedMap(new HashMap<>());
    private final List<Ttl> ttls = Collections.synchronizedList(new ArrayList<>());

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);

    private final AtomicInteger attemptedAcquisitions = new AtomicInteger(0);

    public InMemoryLocking() {
        super("");
    }

    public static void releaseAll() {
        resourcesToCallers.clear();
    }

    @Override
    public boolean acquire(String callerId, String resourceId, Long ttl) throws LightblueException {
        attemptedAcquisitions.incrementAndGet();

        if (resourcesToCallers.putIfAbsent(resourceId, callerId) == null) {
            if (ttl != null) {
                ttls.add(new Ttl(callerId, resourceId, ttl));
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean release(String callerId, String resourceId) throws LightblueException {
        if (callerId.equals(resourcesToCallers.get(resourceId))) {
            resourcesToCallers.remove(resourceId);
            return true;
        }

        return false;
    }

    @Override
    public int getLockCount(String callerId, String resourceId) throws LightblueException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean ping(String callerId, String resourceId) throws LightblueException {
        return ttls.stream()
                .filter(ttl -> ttl.callerId.equals(callerId) && ttl.resourceId.equals(resourceId))
                .map(Ttl::ping)
                .findFirst()
                .orElse(false);
    }

    public int attemptedAcquisitions() {
        return attemptedAcquisitions.get();
    }

    class Ttl {
        final String callerId;
        final String resourceId;
        final long ttl;

        volatile long expireTime;
        ScheduledFuture<Boolean> future;

        Ttl(String callerId, String resourceId, long ttl) {
            expireTime = System.currentTimeMillis() + ttl;

            this.future = scheduleTtl();
            this.callerId = callerId;
            this.resourceId = resourceId;
            this.ttl = ttl;
        }

        boolean ping() {
            if (callerId.equals(resourcesToCallers.get(resourceId))) {
                expireTime = System.currentTimeMillis() + ttl;
                return true;
            }
            return false;
        }

        private ScheduledFuture<Boolean> scheduleTtl() {
            return scheduler.schedule(() -> {
                while (expireTime > System.currentTimeMillis()) {
                    Thread.sleep(100);
                }

                ttls.remove(this);
                return release(callerId, resourceId);
            }, ttl, TimeUnit.MILLISECONDS);
        }
    }
}
