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

import org.apache.camel.builder.RouteBuilder;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class PollingDocumentEventProcessorRoute extends RouteBuilder {
    private final DocumentEventRepository documentEventRepository;
    private final Duration pollingInterval;
    private final int batchSize;
    private final String documentEndpoint;
    private final String failureEndpoint;

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
        from("timer:pollForDocumentEvents?period=" + pollingInterval.get(ChronoUnit.MILLIS))
        .routeId("documentEventsProcessor")
        .process(exchange -> {
            List<? extends DocumentEvent> documentEvents = documentEventRepository
                    .retrievePriorityDocumentEventsUpTo(batchSize);

            List<Future<?>> futureDocs = documentEvents.stream()
                    .map(DocumentEvent::lookupDocument)
                    .collect(Collectors.toList());

            documentEventRepository.markDocumentEventsProcessedOrFailed(documentEvents,
                    Collections.emptyList());

            exchange.getIn().setBody(futureDocs);
        })
        .split(body())
        // TODO: add back error handling when we have error info in results
        .to(documentEndpoint);
    }
}
