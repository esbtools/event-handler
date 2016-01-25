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
import java.time.temporal.ChronoUnit;
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
        from("timer:pollForDocumentEvents?period=" + pollingInterval.toMillis())
        .routeId("documentEventsProcessor-" + idCounter.getAndIncrement())
        .process(exchange -> {
            List<? extends DocumentEvent> documentEvents = documentEventRepository
                    .retrievePriorityDocumentEventsUpTo(batchSize);
            Map<DocumentEvent, Future<?>> eventsToFutureDocuments =
                    new HashMap<>(documentEvents.size());

            // Intentionally cache all futures before resolving them.
            for (DocumentEvent event : documentEvents) {
                eventsToFutureDocuments.put(event, event.lookupDocument());
            }

            List<Object> documents = new ArrayList<>(documentEvents.size());
            List<DocumentEvent> successfulEvents = new ArrayList<>(documentEvents.size());
            List<FailedDocumentEvent> failedEvents = new ArrayList<>();

            for (Map.Entry<DocumentEvent, Future<?>> eventToFutureDocument
                    : eventsToFutureDocuments.entrySet()) {
                DocumentEvent event = eventToFutureDocument.getKey();
                Future<?> futureDoc = eventToFutureDocument.getValue();

                try {
                    documents.add(futureDoc.get());
                    successfulEvents.add(event);
                } catch (ExecutionException | InterruptedException e) {
                    failedEvents.add(new FailedDocumentEvent(event, e));
                }
            }

            // FIXME: If this fails, we should attempt to "rollback" these events to be
            // available to the next retrieve events call
            documentEventRepository.markDocumentEventsProcessedOrFailed(successfulEvents, failedEvents);

            exchange.getIn().setBody(Iterables.concat(documents, failedEvents));
        })
        .split(body())
        .streaming()
        .choice()
            .when(e -> e.getIn().getBody() instanceof FailedDocumentEvent).to(failureEndpoint)
            .otherwise().to(documentEndpoint);
    }
}
