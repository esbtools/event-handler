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

import java.io.Closeable;

public interface LockedResource extends Closeable {
    /**
     * Checks if the lock is still acquired.
     *
     * <p>Otherwise, throws {@link LostLockException}, which you should catch if you need to perform
     * any clean up around losing the lock. The thrown exception will include the provided
     * {@code lostLockMessage} which should explain what will happen as a result of the lock being
     * lost.
     *
     * <p>If the ping fails for another reason, a {@link LostLockException} is also thrown, with the
     * underlying failure as its cause.
     */
    void ping(String lostLockMessage) throws LostLockException;
}
