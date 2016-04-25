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

package org.esbtools.eventhandler.lightblue.locking;

import java.io.Closeable;

public interface LockedResource<T> extends Closeable {
    /**
     * Checks if the lock is still acquired or throws a {@link LostLockException}.
     *
     * <p>Code that relies on a lock should perform this check at critical points.
     *
     * <p>You should catch the {@code LostLockException} if you need to perform any clean up around
     * losing the lock. The thrown exception will include the provided {@code lostLockMessage} which
     * should explain what will happen as a result of the lock being lost.
     *
     * <p>If the check fails for another reason, we pessimistically assume the lock was lost, so
     * a {@link LostLockException} is also thrown, with the underlying failure as its cause.
     *
     * <p>In any case that this method throws {@code LostLockException}, no further resource clean
     * up is necessary. The implementation should take care of any necessary cleanup on behalf of the
     * client since we are giving up on the lock... it is lost, after all.
     */
    void ensureAcquiredOrThrow(String lostLockMessage) throws LostLockException;

    T getResource();
}
