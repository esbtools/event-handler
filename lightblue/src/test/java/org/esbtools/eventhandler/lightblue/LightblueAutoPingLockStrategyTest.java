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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.esbtools.eventhandler.lightblue.testing.InMemoryLocking;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(JUnit4.class)
public class LightblueAutoPingLockStrategyTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    InMemoryLocking client1Locking = new InMemoryLocking();
    InMemoryLocking client2Locking = new InMemoryLocking();

    // TODO: The multi client paradigm is not necessary actually since we use unique caller id per
    // call.

    LightblueAutoPingLockStrategy client1StrategyWith2SecondPing = new LightblueAutoPingLockStrategy(
            client1Locking, Duration.ofSeconds(2));
    LightblueAutoPingLockStrategy client2StrategyWith2SecondPing = new LightblueAutoPingLockStrategy(
            client2Locking, Duration.ofSeconds(2));

    LightblueAutoPingLockStrategy client1StrategyWithHalfSecondTtl =
            new LightblueAutoPingLockStrategy(client1Locking,
                    Duration.ofMillis(100), Duration.ofMillis(500));

    ExecutorService executor = Executors.newFixedThreadPool(5);

    /**
     * Populate with any acquired or potentially acquired resources to ensure they are cleaned up.
     *
     * @see #shutdownExecutorAndReleaseLocks()
     */
    List<LockedResource> lockedResources = new ArrayList<>();

    @After
    public void shutdownExecutorAndReleaseLocks() {
        executor.shutdown();

        lockedResources.forEach((lockedResource) -> {
            try {
                lockedResource.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        InMemoryLocking.releaseAll();
    }

    @Test(expected = LockNotAvailableException.class)
    public void shouldNotLetTwoResourcesBeAcquiredSimultaneouslyByDifferentClients()
            throws Exception {
        try {
            lockedResources.add(client1StrategyWith2SecondPing.tryAcquire("resourceAbc"));
        } catch (LockNotAvailableException e) {
            throw new AssertionError("Couldn't get initial lock!", e);
        }

        lockedResources.add(client2StrategyWith2SecondPing.tryAcquire("resourceAbc"));
    }

    @Test(expected = LockNotAvailableException.class)
    public void shouldNotLetTwoResourcesBeAcquiredSimultaneouslyByTheSameClient()
            throws Exception {
        try {
            lockedResources.add(client1StrategyWith2SecondPing.tryAcquire("resourceAbc"));
        } catch (LockNotAvailableException e) {
            throw new AssertionError("Couldn't get initial lock!", e);
        }

        lockedResources.add(client1StrategyWith2SecondPing.tryAcquire("resourceAbc"));
    }

    @Test
    public void shouldNotLetTwoResourcesBeAcquiredSimultaneouslyByTheSameClientOnDifferentThreads()
            throws Exception {
        try {
            lockedResources.add(client1StrategyWith2SecondPing.tryAcquire("resourceAbc"));
        } catch (LockNotAvailableException e) {
            throw new AssertionError("Couldn't get initial lock!", e);
        }

        expectedException.expectCause(Matchers.instanceOf(LockNotAvailableException.class));

        executor.submit(() -> {
            LockedResource<String> resourceAbc = client1StrategyWith2SecondPing.tryAcquire("resourceAbc");
            lockedResources.add(resourceAbc);
            return resourceAbc;
        }).get();
    }

    @Test
    public void shouldMaintainTheLockPastItsOriginalTtlIfItIsNotReleased()
            throws Exception {
        lockedResources.add(client1StrategyWithHalfSecondTtl.tryAcquire("resourceAbc"));

        // Sleep past TTL.
        Thread.sleep(2000);

        assertFalse("The lock expired!", client2Locking.acquire("resourceAbc"));
    }

    @Test
    public void shouldReleaseLocks() throws Exception {
        LockedResource lock = client1StrategyWithHalfSecondTtl.tryAcquire("resourceAbc");

        lockedResources.add(lock);

        lock.close();

        assertTrue(client2Locking.acquire("resourceAbc"));
    }

    @Test
    public void shouldAcquireMultipleIndependentLocks() throws Exception {
        lockedResources.add(client1StrategyWithHalfSecondTtl.tryAcquire("resource1"));
        lockedResources.add(client1StrategyWithHalfSecondTtl.tryAcquire("resource2"));

        assertFalse(client2Locking.acquire("resource1"));
        assertFalse(client2Locking.acquire("resource2"));
    }

    @Test
    public void shouldConfirmLockIsStillAcquiredFromResource() throws Exception {
        LockedResource lock = client1StrategyWithHalfSecondTtl.tryAcquire("resourceAbc");

        lockedResources.add(lock);

        try {
            lock.ensureAcquiredOrThrow("should not be lost");
        } catch (LostLockException e) {
            fail("Erroneously reported lost lock");
        }
    }

    @Test(expected = LostLockException.class)
    public void shouldThrowLostExceptionFromResourceCheckIfLockIsLost() throws Exception {
        LockedResource lock = client1StrategyWithHalfSecondTtl.tryAcquire("resourceAbc");

        lockedResources.add(lock);

        InMemoryLocking.releaseAll();

        lock.ensureAcquiredOrThrow("should throw");
    }

    @Test
    public void shouldAllowDifferentClientsToAcquireSeparateLocks() throws Exception {
        lockedResources.add(client1StrategyWith2SecondPing.tryAcquire("resource1"));
        lockedResources.add(client2StrategyWith2SecondPing.tryAcquire("resource2"));
    }
}
