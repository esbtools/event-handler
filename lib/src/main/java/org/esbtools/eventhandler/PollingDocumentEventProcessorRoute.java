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
import org.apache.camel.builder.RouteBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
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
        .routeId("documentEventsProcessor-" + id)
        .process(exchange -> {
            List<? extends DocumentEvent> documentEvents = documentEventRepository
                    .retrievePriorityDocumentEventsUpTo(batchSize);
            Map<DocumentEvent, Future<?>> eventsToFutureDocuments =
                    new HashMap<>(documentEvents.size());

            // Intentionally cache all futures before resolving them.
            for (DocumentEvent event : documentEvents) {
                eventsToFutureDocuments.put(event, event.lookupDocument());
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
                    failedEvents.add(new FailedDocumentEvent(event, e));
                }
            }

            documentEventRepository
                    .checkExpired(eventsToDocuments.keySet())
                    .forEach(eventsToDocuments::remove);

            log.debug("Publishing on route {}: {}", exchange.getFromRouteId(), eventsToDocuments.values());

            // TODO: Only update failures here. Rest should be updated in callback post-enqueue.
            // That we truly know if successfully published or not.
            documentEventRepository.markDocumentEventsProcessedOrFailed(eventsToDocuments.keySet(), failedEvents);

            exchange.getIn().setBody(Iterables.concat(eventsToDocuments.values(), failedEvents));
        })
        .split(body())
        .streaming()
        .choice()
            .when(e -> e.getIn().getBody() instanceof FailedDocumentEvent).to(failureEndpoint)
            .otherwise().to(documentEndpoint);
    }
}
