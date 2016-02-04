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

/**
 * Abstracts an atomic exclusive locking mechanism which is used to coordinate concurrent work on
 * logical "resources."
 */
public interface LockStrategy {
    /**
     * Attempts to acquire a lock by the provided {@code resourceId} for the logical resource
     * provided by {@code resource}.
     *
     * <p>After a resource is locked, it is expected that nothing else can acquire it again until it
     * is released (intentionally or not), no matter if it is the same thread, another thread, or
     * across a network.
     *
     * @param resourceId A string which identifies the resource and only that resource.
     * @param resource A logically locked resource as a result of the id being locked. Participants
     *                 must agree on a scheme of resourceIds to resources.
     * @param <T> The type of the resource being acquired.
     * @return A wrapper around the locked resource with methods to manage the acquired lock.
     * @throws LockNotAvailableException If for whatever reason the lock could not be acquired.
     * Generally this is simply because another client has the lock, but it could also be due to
     * network failure, etc.
     */
    <T> LockedResource<T> tryAcquire(String resourceId, T resource) throws LockNotAvailableException;

    /**
     * Equivalent to {@link #tryAcquire(String, Object)} except the resource being locked has the
     * knowledge of its own resourceId to lock with.
     *
     * @see Lockable
     */
    default <T extends Lockable> LockedResource<T> tryAcquire(T lockable) throws LockNotAvailableException {
        return tryAcquire(lockable.getResourceId(), lockable);
    }

    /**
     * Equivalent to {@link #tryAcquire(String, Object)} in cases where the resource being locked is
     * not applicable, or is the same as the String {@code resourceId} provided.
     */
    default LockedResource<String> tryAcquire(String resourceId) throws LockNotAvailableException {
        return tryAcquire(resourceId, resourceId);
    }
}
