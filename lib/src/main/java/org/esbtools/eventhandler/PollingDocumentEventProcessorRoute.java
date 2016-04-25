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

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import org.apache.camel.builder.RouteBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class PollingDocumentEventProcessorRoute extends RouteBuilder {
    private final DocumentEventRepository documentEventRepository;
    private final Duration pollingInterval;
    private final int batchSize;
    private final String documentEndpoint;
    private final String failureEndpoint;

    private static final AtomicInteger idCounter = new AtomicInteger(1);
    private final int id = idCounter.getAndIncrement();

    public PollingDocumentEventProcessorRoute(DocumentEventRepository documentEventRepository,
            Duration pollingInterval, int batchSize, String documentEndpoint,
            String failureEndpoint) {
        this.documentEventRepository = documentEventRepository;
        this.pollingInterval = pollingInterval;
        this.batchSize = batchSize;
        this.documentEndpoint = documentEndpoint;
        this.failureEndpoint = failureEndpoint;
    }

    @Override
    public void configure() throws Exception {
        from("timer:pollForDocumentEvents" + id + "?period=" + pollingInterval.toMillis())
        .routeId("documentEventProcessor-" + id)
        .process(exchange -> {
            List<? extends DocumentEvent> documentEvents = documentEventRepository
                    .retrievePriorityDocumentEventsUpTo(batchSize);
            Map<DocumentEvent, Future<?>> eventsToFutureDocuments =
                    new HashMap<>(documentEvents.size());

            // Intentionally cache all futures before resolving them.
            for (DocumentEvent event : documentEvents) {
                try {
                    eventsToFutureDocuments.put(event, event.lookupDocument());
                } catch (Exception e) {
                    log.error("Failed to get future document for document event: " + event, e);
                    eventsToFutureDocuments.put(event, Futures.immediateFailedFuture(e));
                }
            }

            Map<DocumentEvent, Object> eventsToDocuments = new HashMap<>(documentEvents.size());
            List<FailedDocumentEvent> failedEvents = new ArrayList<>();

            for (Map.Entry<DocumentEvent, Future<?>> eventToFutureDocument
                    : eventsToFutureDocuments.entrySet()) {
                DocumentEvent event = eventToFutureDocument.getKey();
                Future<?> futureDoc = eventToFutureDocument.getValue();

                try {
                    eventsToDocuments.put(event, futureDoc.get());
                } catch (ExecutionException | InterruptedException e) {
                    log.error("Failed to get future document for document event: " + event, e);
                    failedEvents.add(new FailedDocumentEvent(event, e));
                }
            }

            try {
                documentEventRepository.markDocumentEventsPublishedOrFailed(
                        Collections.emptyList(), failedEvents);
            } catch (Exception e) {
                if (log.isErrorEnabled()) {
                    log.error("Failed to update failed events. They will be reprocessed. " +
                            "Failures were: " + failedEvents, e);
                }
            }

            Iterator<Map.Entry<DocumentEvent, Object>> eventsToDocumentsIterator =
                    eventsToDocuments.entrySet().iterator();
            while (eventsToDocumentsIterator.hasNext()) {
                Map.Entry<DocumentEvent, Object> eventToDocument = eventsToDocumentsIterator.next();
                try {
                    documentEventRepository.ensureTransactionActive(eventToDocument.getKey());
                } catch (Exception e) {
                    eventsToDocumentsIterator.remove();
                    if (log.isWarnEnabled()) {
                        log.warn("Event transaction no longer active, not processing: " +
                                eventToDocument.getKey(), e);
                    }
                }
            }

            log.debug("Publishing {} documents on route {}: {}",
                    eventsToDocuments.size(), exchange.getFromRouteId(), eventsToDocuments.values());

            exchange.getIn().setBody(Iterables.concat(eventsToDocuments.entrySet(), failedEvents));
        })
        .split(body())
        .streaming()
        .choice()
            .when(e -> e.getIn().getBody() instanceof FailedDocumentEvent).to(failureEndpoint)
            .otherwise()
                .process(exchange -> {
                    Map.Entry<DocumentEvent, Object> eventToDocument =
                            exchange.getIn().getBody(Map.Entry.class);
                    exchange.setProperty("originalEvent", eventToDocument.getKey());
                    exchange.getIn().setBody(eventToDocument.getValue());
                })
                .to(documentEndpoint)
                // If producing to documentEndpoint succeeded, update original event status...
                // TODO(ahenning): This updates event status one at a time. We could consider using
                // aggregation strategy with splitter to update all in bulk which would take
                // advantage of repository implementations which can update many statuses in one
                // call.
                .process(exchange -> {
                    DocumentEvent event = exchange.getProperty("originalEvent", DocumentEvent.class);

                    if (event == null) {
                        throw new IllegalStateException("Could not get original event from " +
                                "exchange. Won't update event status as published. Exchange was: " +
                                exchange);
                    }

                    documentEventRepository.markDocumentEventsPublishedOrFailed(
                            Collections.singleton(event), Collections.emptyList());
                });
    }
}
