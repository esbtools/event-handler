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

import org.apache.camel.Exchange;
import org.apache.camel.Expression;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.support.ExpressionAdapter;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PollingEventHandlerRoute extends RouteBuilder {
    private final NotificationRepository notificationRepository;
    private final EventRepository eventRepository;
    private final DocumentRepository documentRepository;
    private final Duration newEventPollDelay;
    private final int notificationBatchSize;
    private final Duration readyEventPollDelay;
    private final int readyEventBatchSize;
    private final String toEsbEndpoint;
    private final String toErrorHandlingEndpoint;
    private final LookupResultConverter failedResultConverter;
    private final LookupResultConverter publishedResultConverter;

    public PollingEventHandlerRoute(NotificationRepository notificationRepository,
            EventRepository eventRepository, DocumentRepository documentRepository,
            Duration newEventPollDelay, int notificationBatchSize, Duration readyEventPollDelay,
            int readyEventBatchSize, String toEsbEndpoint, String toErrorHandlingEndpoint) {
        this(notificationRepository, eventRepository, documentRepository, newEventPollDelay,
                notificationBatchSize, readyEventPollDelay, readyEventBatchSize, toEsbEndpoint,
                toErrorHandlingEndpoint, LookupResultConverter.identity(),
                LookupResultConverter.identity());
    }

    public PollingEventHandlerRoute(NotificationRepository notificationRepository,
            EventRepository eventRepository, DocumentRepository documentRepository,
            Duration newEventPollDelay, int notificationBatchSize, Duration readyEventPollDelay,
            int readyEventBatchSize, String toEsbEndpoint, String toErrorHandlingEndpoint,
            LookupResultConverter failedResultConverter,
            LookupResultConverter publishedResultConverter) {
        this.notificationRepository = notificationRepository;
        this.eventRepository = eventRepository;
        this.documentRepository = documentRepository;
        this.newEventPollDelay = newEventPollDelay;
        this.notificationBatchSize = notificationBatchSize;
        this.readyEventPollDelay = readyEventPollDelay;
        this.readyEventBatchSize = readyEventBatchSize;
        this.toEsbEndpoint = toEsbEndpoint;
        this.toErrorHandlingEndpoint = toErrorHandlingEndpoint;
        this.failedResultConverter = failedResultConverter;
        this.publishedResultConverter = publishedResultConverter;
    }

    @Override
    public void configure() throws Exception {
        from("timer:newEvents?delay=" + newEventPollDelay.get(ChronoUnit.MILLIS))
                .routeId("new-events")
                .process(exchange -> {
                    List<Notification> notifications = notificationRepository.
                            retrieveOldestNotificationsUpTo(notificationBatchSize);
                    List<DocumentEvent> documentEvents = new ArrayList<>(notifications.size());

                    for (Notification notification : notifications) {
                        // TODO: Handle exceptions when getting document events
                        Collection<DocumentEvent> newDocEvents = notification.toDocumentEvents();
                        documentEvents.addAll(newDocEvents);
                    }

                    eventRepository.addNewDocumentEvents(documentEvents);
                    notificationRepository.confirmProcessed(notifications);
                });

        from("timer:readyEvents?delay=" + readyEventPollDelay.get(ChronoUnit.MILLIS))
                .routeId("ready-events")
                .process(exchange -> {
                    // TODO: Should event repository just lookup the entities in this design?
                    List<DocumentEvent> readyEvents = eventRepository.
                            retrievePriorityDocumentEventsUpTo(readyEventBatchSize);

                    // TODO: If this fails to return results, should put events back in ready pool
                    // or fail them?
                    Collection<LookupResult> lookupResults = documentRepository.
                            lookupDocumentsForEvents(readyEvents);

                    eventRepository.confirmPublishedOrFailedFromLookupResults(lookupResults);

                    exchange.getIn().setBody(lookupResults);
                })
                .split(body())
                .choice()
                .when(e -> e.getIn().getBody(SimpleLookupResult.class).hasErrors())
                    .setBody(usingLookupResultConverter(failedResultConverter))
                    .to(toErrorHandlingEndpoint)
                .otherwise()
                    .setBody(usingLookupResultConverter(publishedResultConverter))
                    .to(toEsbEndpoint);
    }

    private Expression usingLookupResultConverter(LookupResultConverter resultConverter) {
        return new ExpressionAdapter() {
            @Override
            public Object evaluate(Exchange exchange) {
                LookupResult lookupResult = exchange.getIn().getBody(SimpleLookupResult.class);
                return resultConverter.evaluate(lookupResult);
            }
        };
    }
}
