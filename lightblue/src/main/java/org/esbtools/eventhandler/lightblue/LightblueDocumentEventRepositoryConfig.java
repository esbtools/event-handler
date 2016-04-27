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

import org.esbtools.eventhandler.DocumentEvent;
import org.esbtools.eventhandler.DocumentEventRepository;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

public interface LightblueDocumentEventRepositoryConfig {
    /**
     * Governs whether or not document events are processed based on their type.
     *
     * <p>A repository will never process types which it is not configured to support.
     */
    Set<String> getCanonicalTypesToProcess();

    /**
     * Not to be confused with the maximum number of document events passed to
     * {@link DocumentEventRepository#retrievePriorityDocumentEventsUpTo(int)}, this governs the
     * max batch size of events fetched from lightblue and available for optimization.
     *
     * <p>For example, if you ask for 50 document events to be retrieved, and your batch size is
     * 100, we will initially fetch 100 document events (assuming there are >= 100 events waiting to
     * be processed) from lightblue. Among those 100, we will try to optimize away as many events as
     * possible by checking for events which can be merged or superseded. Finally, among those left,
     * we will return the 50 highest priority events. Any remaining events past 50 will be
     * untouched, available for future retrievals.
     *
     * @see DocumentEvent#couldMergeWith(DocumentEvent)
     * @see DocumentEvent#isSupersededBy(DocumentEvent)
     */
    Integer getDocumentEventsBatchSize();

    /**
     * How long can a document event remain processing before we allow it to be retrieved again for
     * reprocessing?
     */
    Duration getDocumentEventProcessingTimeout();

    /**
     * How long before a document event is available for retrieval do we drop the event and let it
     * be reprocessed?
     *
     * <p>In other words, this governs when we stop processing an event in flight because we're too
     * near when another retrieval may see it is past its
     * {@link #getDocumentEventProcessingTimeout()} and retrieve it for reprocessing.
     *
     * <p>N.B. The existence of this configuration is a function of our current transaction scheme.
     * This could go away, for instance, if we either atomically updated an event's processing
     * timestamp before publishing its document. Other alternative schemes are possible.
     */
    Duration getDocumentEventExpireThreshold();

    /**
     * When adding new document events, we can make (total new events) / (max events per insert)
     * requests, instead of one request with all new events in a single call. If no integer is
     * provided (the optional is empty), we will do one request with all new events.
     *
     * <p>Setting a limit is recommended as it protects against potentially extremely significant
     * notifications producing a huge quantity of document events and failing to insert them all in
     * one call.
     */
    Optional<Integer> getMaxDocumentEventsPerInsert();
}
