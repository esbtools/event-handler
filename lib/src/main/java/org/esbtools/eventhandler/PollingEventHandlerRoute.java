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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

// TODO: Split out to two routebuilders, one for notifications and one for document events
public class PollingEventHandlerRoute extends RouteBuilder {
    private final NotificationRepository notificationRepository;
    private final EventRepository eventRepository;
    private final Duration newEventPollDelay;
    private final int notificationBatchSize;
    private final Duration readyEventPollDelay;
    private final int readyEventBatchSize;
    private final String toEsbEndpoint;

    public PollingEventHandlerRoute(NotificationRepository notificationRepository,
            EventRepository eventRepository, Duration newEventPollDelay, int notificationBatchSize,
            Duration readyEventPollDelay, int readyEventBatchSize, String toEsbEndpoint) {
        this.notificationRepository = notificationRepository;
        this.eventRepository = eventRepository;
        this.newEventPollDelay = newEventPollDelay;
        this.notificationBatchSize = notificationBatchSize;
        this.readyEventPollDelay = readyEventPollDelay;
        this.readyEventBatchSize = readyEventBatchSize;
        this.toEsbEndpoint = toEsbEndpoint;
    }

    @Override
    public void configure() throws Exception {
        from("timer:newEvents?delay=" + newEventPollDelay.get(ChronoUnit.MILLIS))
                .routeId("new-events")
                .process(exchange -> {
                    List<Notification> notifications = notificationRepository.
                            retrieveOldestNotificationsUpTo(notificationBatchSize);
                    List<Future<Collection<DocumentEvent>>> allFutureDocEvents =
                            new ArrayList<>(notifications.size());

                    for (Notification notification : notifications) {
                        Future<Collection<DocumentEvent>> futureDocEvents = notification.toDocumentEvents();
                        allFutureDocEvents.add(futureDocEvents);
                    }

                    List<DocumentEvent> documentEvents = new ArrayList<>();

                    for (Future<Collection<DocumentEvent>> futureDocEvents : allFutureDocEvents) {
                        try {
                            documentEvents.addAll(futureDocEvents.get());
                        } catch (ExecutionException | InterruptedException e) {
                            // TODO: fail notification
                            throw e;
                        }
                    }

                    eventRepository.addNewDocumentEvents(documentEvents);
                    notificationRepository.markNotificationsProcessedOrFailed(notifications,
                            Collections.emptyList());
                });

        from("timer:readyEvents?delay=" + readyEventPollDelay.get(ChronoUnit.MILLIS))
                .routeId("ready-events")
                .process(exchange -> {
                    // TODO: Should event repository just lookup the entities in this design?
                    List<? extends DocumentEvent> documentEvents = eventRepository.
                            retrievePriorityDocumentEventsUpTo(readyEventBatchSize);

                    // TODO: If this fails to return results, should put events back in ready pool
                    // or fail them?
                    List<Future<?>> futureDocs = documentEvents.stream()
                            .map(DocumentEvent::lookupDocument)
                            .collect(Collectors.toList());

                    // TODO: discern which docs have failed results
                    // Or add source() API to result and pass result here
                    eventRepository.markDocumentEventsProcessedOrFailed(documentEvents,
                            Collections.emptyList());

                    exchange.getIn().setBody(futureDocs);
                })
                .split(body())
                // TODO: add back error handling when we have error info in results
                .to(toEsbEndpoint);
    }
}
