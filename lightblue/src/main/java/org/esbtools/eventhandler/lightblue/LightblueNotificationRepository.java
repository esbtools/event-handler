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

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.Locking;
import com.redhat.lightblue.client.request.DataBulkRequest;
import com.redhat.lightblue.client.response.LightblueException;

import org.esbtools.eventhandler.EventHandlerException;
import org.esbtools.eventhandler.FailedNotification;
import org.esbtools.eventhandler.Notification;
import org.esbtools.eventhandler.NotificationRepository;
import org.esbtools.lightbluenotificationhook.NotificationEntity;

import java.sql.Date;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class LightblueNotificationRepository implements NotificationRepository {
    private final LightblueClient lightblue;
    private final String[] entities;
    private final Locking locking;
    private final NotificationFactory notificationFactory;
    private final Clock clock;

    public LightblueNotificationRepository(LightblueClient lightblue, String[] entities,
            String lockingDomain, NotificationFactory notificationFactory, Clock clock) {
        this.lightblue = lightblue;
        this.entities = entities;
        this.notificationFactory = notificationFactory;
        this.clock = clock;

        locking = lightblue.getLocking(lockingDomain);
    }

    @Override
    public List<LightblueNotification> retrieveOldestNotificationsUpTo(int maxEvents)
            throws LightblueException {
        try {
            blockUntilLockAcquired(Locks.forNotificationsForEntities(entities));

            BulkLightblueRequester requester = new BulkLightblueRequester(lightblue);

            NotificationEntity[] notificationEntities = lightblue
                    .data(FindRequests.oldestNotificationsForEntitiesUpTo(entities, maxEvents))
                    .parseProcessed(NotificationEntity[].class);

            for (NotificationEntity entity : notificationEntities) {
                entity.setStatus(NotificationEntity.Status.processing);
            }

            DataBulkRequest updateEntities = new DataBulkRequest();
            updateEntities.addAll(UpdateRequests.notificationsStatusAndProcessedDate(
                    Arrays.asList(notificationEntities)));
            // If this fails, intentionally let propagate and release lock.
            // Another thread, or another poll, will try again.
            lightblue.bulkData(updateEntities);

            return Arrays.stream(notificationEntities)
                    .map(entity -> notificationFactory.getNotificationForEntity(entity, requester))
                    .collect(Collectors.toList());
        } finally {
            locking.release(Locks.forNotificationsForEntities(entities));
        }
    }

    @Override
    public void markNotificationsProcessedOrFailed(Collection<? extends Notification> notification,
            Collection<FailedNotification> failures) throws LightblueException {
        List<NotificationEntity> processedNotificationEntities = notification.stream()
                .map(LightblueNotificationRepository::asEntity)
                .peek(entity -> {
                    entity.setStatus(NotificationEntity.Status.processed);
                    entity.setProcessedDate(Date.from(clock.instant()));
                })
                .collect(Collectors.toList());

        List<NotificationEntity> failedNotificationEntities = failures.stream()
                .map(FailedNotification::notification)
                .map(LightblueNotificationRepository::asEntity)
                .peek(entity -> {
                    entity.setStatus(NotificationEntity.Status.failed);
                    entity.setProcessedDate(Date.from(clock.instant()));
                })
                .collect(Collectors.toList());

        DataBulkRequest markNotifications = new DataBulkRequest();
        markNotifications.addAll(
                UpdateRequests.notificationsStatusAndProcessedDate(processedNotificationEntities));
        markNotifications.addAll(
                UpdateRequests.notificationsStatusAndProcessedDate(failedNotificationEntities));

        // TODO: Deal with failures
        // Waiting on: https://github.com/lightblue-platform/lightblue-client/issues/202
        lightblue.bulkData(markNotifications);
    }

    private void blockUntilLockAcquired(String resourceId) throws LightblueException {
        while (!locking.acquire(resourceId)) {
            try {
                // TODO: Parameterize lock polling interval
                // Or can we do lock call that only returns once lock is available?
                Thread.sleep(3000L);
            } catch (InterruptedException e) {
                // Just try again...
            }
        }
    }

    private static NotificationEntity asEntity(Notification notification) {
        if (notification instanceof LightblueNotification) {
            return ((LightblueNotification) notification).wrappedNotificationEntity();
        }

        throw new EventHandlerException("Unknown notification type. Only LightblueNotification " +
                "are supported. Event type was: " + notification.getClass());
    }
}
