/*
 *  Copyright 2015 esbtools Contributors and/or its affiliates.
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

package org.esbtools.eventhandler;

import java.util.Collection;

public interface DocumentRepository {
    /**
     * Retrieves documents containing <em>latest</em> state of data represented by the document
     * events.
     *
     * <p>Subsequent lookups will include newly persisted changes to the data.
     *
     * <p>A document is a representation of some underlying data which is able to be shared. It is a
     * function of the needs of the canonical data model for the given document type.
     */
    Collection<LookupResult> lookupDocumentsForEvents(Collection<DocumentEvent> documentEvents)
            throws Exception;
}
