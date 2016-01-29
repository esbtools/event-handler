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
 * Abstracts a transactional backing store and staging area for {@link DocumentEvent events}.
 *
 * <p>A repository is responsible for handling CRUD and data parsing operations around these
 * objects, in whatever scheme necessary.
 *
 * <p>Production implementations are expected to be thread safe, even across a network.
 */
public interface DocumentEventRepository {

    /**
     * Persists new document events, updating originating notifications if applicable.
     *
     * <p>The document events are retrievable from {@link #retrievePriorityDocumentEventsUpTo(int)}.
     */
    void addNewDocumentEvents(Collection<? extends DocumentEvent> documentEvents) throws Exception;

    /**
     * Retrieve the top {@code maxEvents} document events in priority order. Document events are
     * created by calling {@link #addNewDocumentEvents(Collection)}.
     *
     * <p>Document events are expected to be fully optimized and ready to be published
     * immediately. That is, the returned list should contain only unique, merged events, in
     * priority order. Events which were superseded or made obsolete by a merge operation should not
     * be included.
     *
     * <p>Subsequent calls should do their best to return unique sets, even among multiple threads.
     *
     * <p>Retrieved document events begin a transaction with those events. Calling
     * {@link #markDocumentEventsProcessedOrFailed(Collection, Collection)} ends this transaction
     * on the provided events. This transaction may end for other reasons, such as a distributed
     * lock failure or timeout, which would cause subsequent calls to retrieve these same events
     * again. To work around this, before documents are published, {@link #checkExpired(Collection)}
     * is called in order to determine if any transactions may have ended prematurely.
     */
    List<? extends DocumentEvent> retrievePriorityDocumentEventsUpTo(int maxEvents) throws Exception;

    /**
     * Among the provided events, looks for those who's transactions started by
     * {@link #retrievePriorityDocumentEventsUpTo(int)} may have ended prematurely.
     *
     * <p>Transactions can end before processed or failure confirmation for a variety of reasons,
     * such as network failure or timeout, depending on the implementation.
     *
     * @param events Events to check for expiration. Will not be mutated.
     * @return A list that has all source events which are expired.
     */
    Collection<? extends DocumentEvent> checkExpired(Collection<? extends DocumentEvent> events);

    void markDocumentEventsProcessedOrFailed(Collection<? extends DocumentEvent> events,
            Collection<FailedDocumentEvent> failures) throws Exception;
}
