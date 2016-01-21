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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.jayway.awaitility.Awaitility;
import org.esbtools.eventhandler.lightblue.testing.InMemoryLocking;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RunWith(JUnit4.class)
public class LightblueAutoPingLockStrategyTest {
    InMemoryLocking client1Locking = new InMemoryLocking();
    InMemoryLocking client2Locking = new InMemoryLocking();

    LightblueAutoPingLockStrategy client1StrategyWith2SecondPing = new LightblueAutoPingLockStrategy(
            client1Locking, Duration.ofMillis(100), Duration.ofSeconds(2));
    LightblueAutoPingLockStrategy client2StrategyWith2SecondPing = new LightblueAutoPingLockStrategy(
            client2Locking, Duration.ofMillis(100), Duration.ofSeconds(2));

    LightblueAutoPingLockStrategy client1StrategyWithHalfSecondTtl =
            new LightblueAutoPingLockStrategy(client1Locking, Duration.ofMillis(100),
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

    @Test(timeout = 4000)
    public void shouldBlockUntilLockAcquired() throws Exception {
        client1Locking.acquire("resource1");

        // In another thread, release lock after some delay...
        executor.submit(() -> {
            try {
                Thread.sleep(2000);
                InMemoryLocking.releaseAll();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        });

        // Should unblock eventually...
        lockedResources.add(client2StrategyWith2SecondPing.blockUntilAcquired("resource1"));

        assertFalse(client1Locking.acquire("resource1"));
    }

    @Test(expected = TimeoutException.class)
    public void shouldNotLetTwoResourcesBeAcquiredSimultaneously()
            throws Exception {
        lockedResources.add(client1StrategyWith2SecondPing.blockUntilAcquired("resourceAbc"));

        executor.submit(() ->
                lockedResources.add(client2StrategyWith2SecondPing.blockUntilAcquired("resourceAbc")))
                .get(1, TimeUnit.SECONDS);
    }

    @Test
    public void shouldMaintainTheLockPastItsOriginalTtlIfItIsNotReleased()
            throws Exception {
        lockedResources.add(client1StrategyWithHalfSecondTtl.blockUntilAcquired("resourceAbc"));

        // Sleep past TTL.
        Thread.sleep(2000);

        assertFalse("The lock expired!", client2Locking.acquire("resourceAbc"));
    }

    @Test
    public void shouldReleaseLocks() throws Exception {
        LockedResource lock = client1StrategyWithHalfSecondTtl.blockUntilAcquired("resourceAbc");

        lockedResources.add(lock);

        lock.close();

        assertTrue(client2Locking.acquire("resourceAbc"));
    }

    @Test
    public void shouldAcquireMultipleIndependentLocks() throws Exception {
        lockedResources.add(client1StrategyWithHalfSecondTtl.blockUntilAcquired("resource1", "resource2"));

        assertFalse(client2Locking.acquire("resource1"));
        assertFalse(client2Locking.acquire("resource2"));
    }

    @Test
    public void shouldReleaseAllLocksWhenMultipleAcquiredAsOneResource() throws Exception {
        LockedResource aggregateLock = client1StrategyWithHalfSecondTtl.blockUntilAcquired("resource1", "resource2");

        lockedResources.add(aggregateLock);

        aggregateLock.close();

        assertTrue(client2Locking.acquire("resource1"));
        assertTrue(client2Locking.acquire("resource2"));
    }

    @Test
    public void shouldConfirmLockIsStillAcquiredFromResource() throws Exception {
        LockedResource lock = client1StrategyWithHalfSecondTtl.blockUntilAcquired("resourceAbc");

        lockedResources.add(lock);

        try {
            lock.ensureAcquiredOrThrow("should not be lost");
        } catch (LostLockException e) {
            fail("Erroneously reported lost lock");
        }
    }

    @Test(expected = LostLockException.class)
    public void shouldThrowLostExceptionFromResourceCheckIfLockIsLost() throws Exception {
        LockedResource lock = client1StrategyWithHalfSecondTtl.blockUntilAcquired("resourceAbc");

        lockedResources.add(lock);

        InMemoryLocking.releaseAll();

        lock.ensureAcquiredOrThrow("should throw");
    }

    @Test(timeout = 1000)
    public void shouldAllowDifferentClientsToAcquireSeparateLocks() throws Exception {
        lockedResources.add(client1StrategyWith2SecondPing.blockUntilAcquired("resource1"));
        lockedResources.add(client2StrategyWith2SecondPing.blockUntilAcquired("resource2"));
    }

    @Test(expected = LostLockException.class)
    public void shouldThrowLostLockExceptionFromAggregateResourceCheckIfJustOneLockIsLost() throws Exception {
        LockedResource lock = client1StrategyWithHalfSecondTtl.blockUntilAcquired("resource1", "resource2");

        lockedResources.add(lock);

        client1Locking.release("resource1");

        lock.ensureAcquiredOrThrow("should throw");
    }

    @Test
    public void shouldKeepPingingOtherLocksInAggregateDespiteOneBeingLost() throws Exception {
        LockedResource lock = client1StrategyWithHalfSecondTtl.blockUntilAcquired("resource1", "resource2");

        lockedResources.add(lock);

        client1Locking.release("resource1");

        // Sleep past TTL
        Thread.sleep(2000);

        assertFalse(client2Locking.acquire("resource2"));
    }

    @Test
    public void shouldThrowLostLockExceptionWithAllLostLockExceptionsFromAggregateResourceCheckIfMultipleLocksAreLost()
        throws Exception {
        LockedResource lock = client1StrategyWithHalfSecondTtl.blockUntilAcquired("resource1", "resource2");

        lockedResources.add(lock);

        InMemoryLocking.releaseAll();

        try {
            lock.ensureAcquiredOrThrow("should throw");
        } catch (LostLockException e) {
            assertThat(e.lostLocks()).hasSize(2);
            assertThat(e.lostLocks()).containsNoDuplicates();
        }
    }

    @Test(timeout = 1000)
    public void shouldReleaseAnyAcquiredLocksIfUnableToAcquireAllLocks() throws Exception {
        client1Locking.acquire("resource1");

        // This should block forever, so perform in another thread...
        executor.submit(() -> lockedResources.add(
                client2StrategyWith2SecondPing.blockUntilAcquired("resource1", "resource2")));

        // Wait until the locks are attempted a few times in other thread...
        Awaitility.await().until(() -> client2Locking.attemptedAcquisitions(), Matchers.greaterThan(5));

        // Client2 should not hog this lock while it's trying to get both...
        client1StrategyWith2SecondPing.blockUntilAcquired("resource2");
    }
}
