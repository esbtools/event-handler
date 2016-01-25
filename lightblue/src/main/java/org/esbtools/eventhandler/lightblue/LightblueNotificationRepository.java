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
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.Locking;
import com.redhat.lightblue.client.request.DataBulkRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.esbtools.eventhandler.EventHandlerException;
import org.esbtools.eventhandler.FailedNotification;
import org.esbtools.eventhandler.Notification;
import org.esbtools.eventhandler.NotificationRepository;
import org.esbtools.lightbluenotificationhook.NotificationEntity;

import java.sql.Date;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

public class LightblueNotificationRepository implements NotificationRepository {
    private final LightblueClient lightblue;
    private final LightblueNotificationRepositoryConfig config;
    private final LockStrategy lockStrategy;
    private final Map<String, NotificationFactory> notificationFactoryByEntityName;
    private final Clock clock;

    private final Set<String> supportedEntityNames;
    /** Cached to avoid extra garbage. */
    private final String[] supportedEntityNamesArray;

    private static final Logger logger = LoggerFactory.getLogger(LightblueNotificationRepository.class);

    public LightblueNotificationRepository(LightblueClient lightblue, LockStrategy lockStrategy,
            LightblueNotificationRepositoryConfig config,
            Map<String, NotificationFactory> notificationFactoryByEntityName, Clock clock) {
        this.lightblue = lightblue;
        this.lockStrategy = lockStrategy;
        this.config = config;
        this.notificationFactoryByEntityName = notificationFactoryByEntityName;
        this.clock = clock;

        supportedEntityNames = notificationFactoryByEntityName.keySet();
        supportedEntityNamesArray = supportedEntityNames.toArray(new String[supportedEntityNames.size()]);
    }

    @Override
    public List<LightblueNotification> retrieveOldestNotificationsUpTo(int maxNotifications)
            throws Exception {
        String[] entitiesToProcess = getSupportedAndEnabledEntityNames();

        if (entitiesToProcess.length == 0) {
            logger.info("Not retrieving any notifications because either there are no enabled " +
                    "or supported entity names to process. Supported entity names are {}. " +
                    "Of those, enabled entity names are {}",
                    supportedEntityNames, Arrays.toString(entitiesToProcess));
            return Collections.emptyList();
        }

        if (maxNotifications == 0) {
            return Collections.emptyList();
        }

        try (LockedResource lock = lockStrategy
                .blockUntilAcquired(ResourceIds.forNotificationsForEntities(entitiesToProcess))) {
            BulkLightblueRequester requester = new BulkLightblueRequester(lightblue);

            NotificationEntity[] notificationEntities = lightblue
                    .data(FindRequests.oldestNotificationsForEntitiesUpTo(entitiesToProcess, maxNotifications))
                    .parseProcessed(NotificationEntity[].class);

            if (notificationEntities.length == 0) {
                return Collections.emptyList();
            }

            for (NotificationEntity entity : notificationEntities) {
                entity.setStatus(NotificationEntity.Status.processing);
            }

            DataBulkRequest updateEntities = new DataBulkRequest();
            updateEntities.addAll(UpdateRequests.notificationsStatusAndProcessedDate(
                    Arrays.asList(notificationEntities)));

            lock.ensureAcquiredOrThrow("Will not process retrieved notifications.");

            // If this fails, intentionally let propagate and release lock.
            // Another thread, or another poll, will try again.
            lightblue.bulkData(updateEntities);

            // TODO: This work should be done before status updates so we can populate failures
            return Arrays.stream(notificationEntities)
                    .map(entity -> {
                        String entityName = entity.getEntityName();

                        NotificationFactory notificationFactory =
                                notificationFactoryByEntityName.get(entityName);

                        if (notificationFactory == null) {
                            throw new NoSuchElementException("No notification factory found for " +
                                    "notification entity name <" + entityName +">. Notification " +
                                    "entity looks like: " + entity);
                        }

                        return notificationFactory.getNotificationForEntity(entity, requester);
                    })
                    .collect(Collectors.toList());
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

        if (markNotifications.getRequests().isEmpty()) {
            return;
        }

        // TODO: Deal with failures
        // Waiting on: https://github.com/lightblue-platform/lightblue-client/issues/202
        lightblue.bulkData(markNotifications);
    }

    private String[] getSupportedAndEnabledEntityNames() {
        Set<String> entityNamesToProcess = config.getEntityNamesToProcess();

        if (entityNamesToProcess == null) {
            return new String[0];
        }

        if (entityNamesToProcess.containsAll(supportedEntityNames)) {
            return supportedEntityNamesArray;
        }

        List<String> supportedAndEnabled = new ArrayList<>(supportedEntityNames);
        supportedAndEnabled.retainAll(entityNamesToProcess);
        return supportedAndEnabled.toArray(new String[supportedAndEnabled.size()]);
    }

    private static NotificationEntity asEntity(Notification notification) {
        if (notification instanceof LightblueNotification) {
            return ((LightblueNotification) notification).wrappedNotificationEntity();
        }

        throw new EventHandlerException("Unknown notification type. Only LightblueNotification " +
                "are supported. Event type was: " + notification.getClass());
    }
}
