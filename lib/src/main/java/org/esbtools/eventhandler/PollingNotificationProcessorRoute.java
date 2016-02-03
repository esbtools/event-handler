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

package org.esbtools.eventhandler;

import org.apache.camel.builder.RouteBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PollingNotificationProcessorRoute extends RouteBuilder {
    private final NotificationRepository notificationRepository;
    private final DocumentEventRepository documentEventRepository;
    private final Duration pollingInterval;
    private final int batchSize;

    private static final AtomicInteger idCounter = new AtomicInteger(1);
    private final int id = idCounter.getAndIncrement();

    public PollingNotificationProcessorRoute(NotificationRepository notificationRepository,
            DocumentEventRepository documentEventRepository, Duration pollingInterval,
            int batchSize) {
        this.notificationRepository = notificationRepository;
        this.documentEventRepository = documentEventRepository;
        this.pollingInterval = pollingInterval;
        this.batchSize = batchSize;
    }

    @Override
    public void configure() throws Exception {
        from("timer:pollForNotifications" + id + "?period=" + pollingInterval.toMillis())
        .routeId("notificationProcessor-" + id)
        .process(exchange -> {
            List<? extends Notification> notifications =
                    notificationRepository.retrieveOldestNotificationsUpTo(batchSize);
            Map<Notification, Future<Collection<DocumentEvent>>> notificationsToFutureEvents =
                    new HashMap<>(notifications.size());

            // Intentionally cache all futures before waiting for any.
            for (Notification notification : notifications) {
                Future<Collection<DocumentEvent>> futureEvents = notification.toDocumentEvents();
                notificationsToFutureEvents.put(notification, futureEvents);
            }

            Map<Notification, Collection<DocumentEvent>> notificationsToDocumentEvents = new HashMap<>();
            List<FailedNotification> failedNotifications = new ArrayList<>();

            for (Entry<Notification, Future<Collection<DocumentEvent>>> notificationToFutureEvents
                    : notificationsToFutureEvents.entrySet()) {
                Notification notification = notificationToFutureEvents.getKey();
                Future<Collection<DocumentEvent>> futureEvents = notificationToFutureEvents.getValue();

                try {
                    notificationsToDocumentEvents.put(notification, futureEvents.get());
                } catch (ExecutionException | InterruptedException e) {
                    failedNotifications.add(new FailedNotification(notification, e));
                }
            }

            notificationRepository
                    .checkExpired(notificationsToDocumentEvents.keySet())
                    .forEach(notificationsToDocumentEvents::remove);

            log.debug("Persisting document events via route {}: {}",
                    exchange.getFromRouteId(), notificationsToDocumentEvents.values());

            List<DocumentEvent> documentEvents = notificationsToDocumentEvents.values()
                    .stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());

            try {
                documentEventRepository.addNewDocumentEvents(documentEvents);
            } catch (Exception e) {
                // TODO: Handle failures more granularly
                for (Notification notification : notificationsToDocumentEvents.keySet()) {
                    failedNotifications.add(new FailedNotification(notification, e));
                }
                notificationsToDocumentEvents.clear();
            }

            notificationRepository.markNotificationsProcessedOrFailed(
                    notificationsToDocumentEvents.keySet(), failedNotifications);
        });
    }
}
