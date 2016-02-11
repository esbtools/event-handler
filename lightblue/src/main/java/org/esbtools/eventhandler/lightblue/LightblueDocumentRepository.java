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

import org.esbtools.eventhandler.DocumentRepository;
import org.esbtools.eventhandler.lightblue.locking.LockStrategy;
import org.esbtools.eventhandler.lightblue.locking.LockedResource;
import org.esbtools.eventhandler.lightblue.locking.LockedResources;

import java.util.Collection;

public class LightblueDocumentRepository implements DocumentRepository {
    private final LockStrategy lockStrategy;

    public LightblueDocumentRepository(LockStrategy lockStrategy) {
        this.lockStrategy = lockStrategy;
    }

    @Override
    public void insertOrUpdate(Collection<Object> documents) throws Exception {
        try (LockedResources<Document> lockedDocuments = parseAndWaitForLocks(documents)) {

        }
    }
}
