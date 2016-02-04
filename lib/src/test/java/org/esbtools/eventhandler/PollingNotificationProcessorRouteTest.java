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

import org.esbtools.eventhandler.testing.FailingNotification;
import org.esbtools.eventhandler.testing.SimpleInMemoryDocumentEventRepository;
import org.esbtools.eventhandler.testing.SimpleInMemoryNotificationRepository;
import org.esbtools.eventhandler.testing.StringNotification;

import com.google.common.truth.Truth;
import com.jayway.awaitility.Awaitility;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class PollingNotificationProcessorRouteTest extends CamelTestSupport {
    SimpleInMemoryNotificationRepository notificationRepository = new SimpleInMemoryNotificationRepository();
    SimpleInMemoryDocumentEventRepository documentEventRepository = new SimpleInMemoryDocumentEventRepository();

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new PollingNotificationProcessorRoute(notificationRepository, documentEventRepository,
                Duration.ofSeconds(1), 10);
    }

    @Test
    public void shouldTurnNotificationsIntoDocumentEventsAndPersistAtPeriodicIntervals()
            throws InterruptedException {
        notificationRepository.addNotifications(randomNotifications(10));
        Thread.sleep(2000);
        notificationRepository.addNotifications(randomNotifications(10));
        Thread.sleep(2000);
        notificationRepository.addNotifications(randomNotifications(10));

        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(documentEventRepository::getDocumentEvents, Matchers.hasSize(30));
    }

    @Test
    public void shouldMarkSuccessfulNotificationsAsProcessedAndFailedNotificationsAsFailed() {
        List<Notification> notifications = new ArrayList<>(10);
        notifications.addAll(randomFailingNotifications(5));
        notifications.addAll(randomNotifications(5));

        notificationRepository.addNotifications(notifications);

        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(notificationRepository::getFailedNotifications, Matchers.hasSize(5));
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(notificationRepository::getProcessedNotifications, Matchers.hasSize(5));
    }

    @Test
    public void shouldNotLetFailedNotificationsPreventSuccessfulOnesFromBeingPersistedAsDocumentEvents() {
        List<Notification> notifications = new ArrayList<>(10);
        notifications.addAll(randomFailingNotifications(5));
        notifications.addAll(randomNotifications(5));

        notificationRepository.addNotifications(notifications);

        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(documentEventRepository::getDocumentEvents, Matchers.hasSize(5));
    }

    @Test
    public void shouldMarkAllNotificationsAsFailedIfDocumentEventRepositoryFailedToPersistTheEvents()
            throws Exception {
        documentEventRepository.failOnAddingDocumentEvents();

        List<Notification> notifications = new ArrayList<>(10);
        notifications.addAll(randomFailingNotifications(5));
        notifications.addAll(randomNotifications(5));

        notificationRepository.addNotifications(notifications);

        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(notificationRepository::getFailedNotifications, Matchers.hasSize(10));
    }

    @Test
    public void shouldDropNotificationsWhoseTransactionsAreNoLongerActive() throws Exception {
        notificationRepository.considerNoTransactionsActive();
        notificationRepository.addNotifications(randomNotifications(10));

        Thread.sleep(5000);

        Truth.assertThat(documentEventRepository.getDocumentEvents()).isEmpty();
    }

    static List<StringNotification> randomNotifications(int amount) {
        List<StringNotification> notifications = new ArrayList<>(amount);

        for(int i = 0; i < amount; i++) {
            notifications.add(new StringNotification(UUID.randomUUID().toString()));
        }

        return notifications;
    }

    static List<FailingNotification> randomFailingNotifications(int amount) {
        List<FailingNotification> notifications = new ArrayList<>(amount);

        for(int i = 0; i < amount; i++) {
            notifications.add(new FailingNotification());
        }

        return notifications;
    }
}
