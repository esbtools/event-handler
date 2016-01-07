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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class PollingNotificationProcessorRoute extends RouteBuilder {
    private final NotificationRepository notificationRepository;
    private final DocumentEventRepository documentEventRepository;
    private final Duration pollingInterval;
    private final int batchSize;

    public PollingNotificationProcessorRoute(NotificationRepository notificationRepository,
            DocumentEventRepository documentEventRepository, Duration pollingInterval, int batchSize) {
        this.notificationRepository = notificationRepository;
        this.documentEventRepository = documentEventRepository;
        this.pollingInterval = pollingInterval;
        this.batchSize = batchSize;
    }

    @Override
    public void configure() throws Exception {
        from("timer:pollForNotifications?period=" + pollingInterval.get(ChronoUnit.MILLIS))
        .routeId("notificationProcessor")
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

            List<DocumentEvent> documentEvents = new ArrayList<>();
            List<Notification> successfulNotifications = new ArrayList<>(notifications.size());
            List<FailedNotification> failedNotifications = new ArrayList<>();

            for (Map.Entry<Notification, Future<Collection<DocumentEvent>>> notificationToFutureEvents
                    : notificationsToFutureEvents.entrySet()) {
                Notification notification = notificationToFutureEvents.getKey();
                Future<Collection<DocumentEvent>> futureEvents = notificationToFutureEvents.getValue();

                try {
                    documentEvents.addAll(futureEvents.get());
                    successfulNotifications.add(notification);
                } catch (ExecutionException | InterruptedException e) {
                    failedNotifications.add(new FailedNotification(notification, e));
                }
            }

            try {
                documentEventRepository.addNewDocumentEvents(documentEvents);
            } catch (Exception e) {
                for (Notification notification : successfulNotifications) {
                    failedNotifications.add(new FailedNotification(notification, e));
                }
                successfulNotifications.clear();
            }

            notificationRepository.markNotificationsProcessedOrFailed(
                    successfulNotifications, failedNotifications);
        });
    }
}
