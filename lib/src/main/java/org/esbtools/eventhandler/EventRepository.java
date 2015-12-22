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
import java.util.List;

/**
 * Abstracts a backing store and staging area for {@link Notification notifications} and
 * {@link DocumentEvent events}.
 *
 * <p>A repository is responsible for handling CRUD operations around these objects, in whatever
 * scheme necessary. All operations have some constraints around multithreading, and should perform
 * optimizations made available by the {@link DocumentEvent} API such as checking for
 * {@link DocumentEvent#isSupersededBy(DocumentEvent) redundancies} and
 * {@link DocumentEvent#merge(DocumentEvent) merging} any events that can be merged.
 */
public interface EventRepository {

    /**
     * Persists new document events, updating originating notifications if applicable.
     *
     * <p>The document events are retrievable from {@link #retrievePriorityDocumentEventsUpTo(int)}.
     */
    void addNewDocumentEvents(Collection<DocumentEvent> documentEvents) throws Exception;

    /**
     * Retrieve the top {@code maxEvents} document events in priority order. Document events are
     * created by calling {@link #addNewDocumentEvents(Collection)}.
     *
     * <p>Document events are expected to be fully optimized and ready to be published
     * immediately. That is, the returned list should contain only unique, merged events, in
     * priority order. Events which were superseded or made obsolete by a merge operation should not
     * be included.
     *
     * <p>Subsequent calls should always return a unique set, even among multiple threads.
     */
    List<DocumentEvent> retrievePriorityDocumentEventsUpTo(int maxEvents) throws Exception;

    // TODO: Handle failed
    void confirmProcessedDocumentEvents(Collection<DocumentEvent> events) throws Exception;
}
