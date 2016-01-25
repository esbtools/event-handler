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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class LostLockException extends Exception {
    private final List<LockedResource> lostLocks;

    public LostLockException(LockedResource lock, String message) {
        super(message + " [Lock: " + lock + "]");

        this.lostLocks = Collections.singletonList(lock);
    }

    public LostLockException(LockedResource lock, String message, Exception cause) {
        super(message + " [Lock: " + lock + "]", cause);

        lostLocks = Collections.singletonList(lock);
    }

    public LostLockException(List<LostLockException> lostLockExceptions) {
        super();

        this.lostLocks = Collections.unmodifiableList(lostLockExceptions.stream()
                .peek(this::addSuppressed)
                .flatMap(e -> e.lostLocks().stream())
                .collect(Collectors.toList()));
    }

    public List<LockedResource> lostLocks() {
        return lostLocks;
    }
}
