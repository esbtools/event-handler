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
import java.util.concurrent.Future;

/**
 * Represents something potentially significant that has happened which is intended to eventually
 * publish documents as a part of "handling" or "processing" this notification and its corresponding
 * {@link DocumentEvent}s.
 *
 * <p>Notifications are meant to be the place for business logic to be applied around interesting
 * data changes, where the result of that change applied to that business logic is one or more
 * documents to be asynchronously published.
 *
 * <p>For example, if you have user data you wish to share asynchronously with some interested
 * consumers, once a user was inserted or updated you would capture that change as a notification.
 * Then, implement a notification to capture the business logic around what, if any, documents
 * should be published as a result of that change.
 *
 * @see DocumentEvent
 */
public interface Notification {
    /**
     * Immediately returns with a {@link Future} representing the collection of
     * {@link DocumentEvent}s this notification should produce.
     *
     * <p>This method is expected to never throw an exception. Failures should be captured in the
     * returned {@code Future}.
     */
    Future<Collection<DocumentEvent>> toDocumentEvents();
}
