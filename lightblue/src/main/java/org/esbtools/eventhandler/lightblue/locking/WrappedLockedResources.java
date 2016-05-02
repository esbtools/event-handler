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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

class WrappedLockedResources<T> implements LockedResources<T> {
    private final Collection<LockedResource<T>> locks;

    public WrappedLockedResources(Collection<LockedResource<T>> locks) {
        this.locks = locks;
    }

    @Override
    public Collection<LockedResource<T>> getLocks() {
        return Collections.unmodifiableCollection(locks);
    }

    @Override
    public void close() throws IOException {
        if (locks.isEmpty()) {
            return;
        }

        List<IOException> exceptions = new ArrayList<>(0);

        for (LockedResource lock : locks) {
            try {
                lock.close();
            } catch (IOException e) {
                exceptions.add(e);
            }
        }

        if (!exceptions.isEmpty()) {
            if (exceptions.size() == 1) {
                throw exceptions.get(0);
            }

            throw new MultipleIOExceptions(exceptions);
        }
    }
}
