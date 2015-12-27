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

package org.esbtools.eventhandler.lightblue;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.Locking;
import com.redhat.lightblue.client.request.DataBulkRequest;
import com.redhat.lightblue.client.response.LightblueBulkDataResponse;
import com.redhat.lightblue.client.response.LightblueDataResponse;
import com.redhat.lightblue.client.response.LightblueException;

import org.esbtools.eventhandler.DocumentEvent;
import org.esbtools.eventhandler.EventHandlerException;
import org.esbtools.eventhandler.EventRepository;
import org.esbtools.eventhandler.FailedDocumentEvent;
import org.esbtools.eventhandler.FailedNotification;
import org.esbtools.eventhandler.Notification;
import org.esbtools.eventhandler.NotificationRepository;
import org.esbtools.eventhandler.lightblue.model.DocumentEventEntity;
import org.esbtools.lightbluenotificationhook.NotificationEntity;

import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

// TODO: Doesn't have to be same class for this
public class LightblueEventRepository implements EventRepository, NotificationRepository {
    private final LightblueClient lightblue;
    private final String[] entities;
    private final int documentEventBatchSize;
    private final Locking locking;
    private final NotificationFactory notificationFactory;
    private final DocumentEventFactory documentEventFactory;
    private final Clock clock;

    public LightblueEventRepository(LightblueClient lightblue, String[] entities,
            int documentEventBatchSize, String lockingDomain,
            NotificationFactory notificationFactory, DocumentEventFactory documentEventFactory,
            Clock clock) {
        this.lightblue = lightblue;
        this.entities = entities;
        this.documentEventBatchSize = documentEventBatchSize;
        this.notificationFactory = notificationFactory;
        this.documentEventFactory = documentEventFactory;
        this.clock = clock;

        locking = lightblue.getLocking(lockingDomain);
    }

    @Override
    public List<Notification> retrieveOldestNotificationsUpTo(int maxEvents)
            throws LightblueException {
        try {
            // TODO: Either block until lock acquired or throw exception and let caller poll
            // TODO: What happens if app dies before lock release? Do we need TTL and ping?
            blockUntilLockAcquired(Locks.forNotificationsForEntities(entities));

            BulkLightblueRequester requester = new BulkLightblueRequester(lightblue);

            NotificationEntity[] notificationEntities = lightblue
                    .data(FindRequests.newNotificationsForEntitiesUpTo(entities, maxEvents))
                    .parseProcessed(NotificationEntity[].class);

            lightblue.data(UpdateRequests.notificationsAsProcessing(notificationEntities));

            return Arrays.stream(notificationEntities)
                    .map(entity -> notificationFactory.getNotificationForEntity(entity, requester))
                    .collect(Collectors.toList());
        } finally {
            locking.release(Locks.forNotificationsForEntities(entities));
        }
    }

    @Override
    public void markNotificationsProcessedOrFailed(Collection<Notification> notification,
            Collection<FailedNotification> failures) throws Exception {
        List<NotificationEntity> processedNotificationEntities = notification.stream()
                .map(LightblueEventRepository::asEntity)
                .collect(Collectors.toList());

        // TODO: Add field in NotificationEntity for failure messages?
        List<NotificationEntity> failedNotificationEntities = failures.stream()
                .map(FailedNotification::notification)
                .map(LightblueEventRepository::asEntity)
                .collect(Collectors.toList());

        DataBulkRequest markNotifications = new DataBulkRequest();
        markNotifications.add(UpdateRequests.processingNotificationsAsProcessed(processedNotificationEntities));
        markNotifications.add(UpdateRequests.processingNotificationsAsFailed(failedNotificationEntities));

        lightblue.bulkData(markNotifications);
    }

    @Override
    public void addNewDocumentEvents(Collection<? extends DocumentEvent> documentEvents)
            throws LightblueException {
        List<DocumentEventEntity> documentEventEntities = documentEvents.stream()
                .map(LightblueEventRepository::asEntity)
                .collect(Collectors.toList());

        lightblue.data(InsertRequests.documentEventsReturningOnlyIds(documentEventEntities));
    }

    @Override
    public List<LightblueDocumentEvent> retrievePriorityDocumentEventsUpTo(int maxEvents)
            throws LightblueException {
        try {
            blockUntilLockAcquired(Locks.forDocumentEventsForEntities(entities));

            DocumentEventEntity[] documentEventEntities = lightblue
                    .data(FindRequests.priorityDocumentEventsForEntitiesUpTo(entities, documentEventBatchSize))
                    .parseProcessed(DocumentEventEntity[].class);

            List<LightblueDocumentEvent> optimized = new ArrayList<>(maxEvents);
            List<DocumentEventEntity> entitiesToUpdate = new ArrayList<>();
            BulkLightblueRequester requester = new BulkLightblueRequester(lightblue);

            for (DocumentEventEntity newEventEntity : documentEventEntities) {
                newEventEntity.setStatus(DocumentEventEntity.Status.processing);

                entitiesToUpdate.add(newEventEntity);

                LightblueDocumentEvent newEvent = documentEventFactory
                        .getDocumentEventForEntity(newEventEntity, requester);

                Iterator<LightblueDocumentEvent> previousEventIterator = optimized.iterator();
                while (previousEventIterator.hasNext()) {
                    LightblueDocumentEvent previousEvent = previousEventIterator.next();

                    if (newEvent.isSupersededBy(previousEvent)) {
                        DocumentEventEntity previousEntity = previousEvent.wrappedDocumentEventEntity();
                        previousEntity.addSurvivorOfIds(newEventEntity.getSurvivorOfIds());
                        previousEntity.addSurvivorOfIds(newEventEntity.get_id());

                        newEventEntity.setStatus(DocumentEventEntity.Status.superseded);
                        newEventEntity.setProcessedDate(ZonedDateTime.now(clock));
                        newEvent = null;
                        break;
                    } else if (newEvent.couldMergeWith(previousEvent)) {
                        LightblueDocumentEvent merger = newEvent.merge(previousEvent);

                        newEventEntity.setStatus(DocumentEventEntity.Status.merged);
                        newEventEntity.setProcessedDate(ZonedDateTime.now(clock));

                        DocumentEventEntity previousEntity = previousEvent.wrappedDocumentEventEntity();
                        previousEntity.setStatus(DocumentEventEntity.Status.merged);
                        previousEntity.setProcessedDate(ZonedDateTime.now(clock));

                        DocumentEventEntity mergerEntity = merger.wrappedDocumentEventEntity();
                        mergerEntity.setStatus(DocumentEventEntity.Status.processing);
                        mergerEntity.addSurvivorOfIds(previousEntity.getSurvivorOfIds());
                        mergerEntity.addSurvivorOfIds(newEventEntity.getSurvivorOfIds());
                        if (previousEntity.get_id() != null) {
                            mergerEntity.addSurvivorOfIds(previousEntity.get_id());
                        }
                        if (newEventEntity.get_id() != null) {
                            mergerEntity.addSurvivorOfIds(newEventEntity.get_id());
                        }

                        newEvent = merger;
                        previousEventIterator.remove();
                    }
                }

                if (newEvent != null) {
                    optimized.add(newEvent);
                }

                // TODO: This could be optimized a little better but will improve this soon after tests
                if (optimized.size() >= maxEvents) {
                    break;
                }
            }

            DataBulkRequest insertAndUpdateEvents = new DataBulkRequest();
            List<LightblueDocumentEvent> newEvents = new ArrayList<>();

            for (LightblueDocumentEvent event : optimized) {
                DocumentEventEntity entity = event.wrappedDocumentEventEntity();
                if (entity.get_id() == null) {
                    newEvents.add(event);
                    insertAndUpdateEvents.add(InsertRequests.documentEventsReturningOnlyIds(entity));
                }
            }

            insertAndUpdateEvents.addAll(UpdateRequests.documentEventsStatusAndProcessedDate(entitiesToUpdate));

            // TODO: Verify these were all successful
            LightblueBulkDataResponse bulkResponse = lightblue.bulkData(insertAndUpdateEvents);
            List<LightblueDataResponse> responses = bulkResponse.getResponses();

            for (int i = 0; i < newEvents.size(); i++) {
                LightblueDataResponse response = responses.get(i);
                LightblueDocumentEvent newEvent = newEvents.get(i);
                DocumentEventEntity newEntity = response.parseProcessed(DocumentEventEntity.class);
                newEvent.wrappedDocumentEventEntity().set_id(newEntity.get_id());
            }

            return optimized;
        } finally {
            locking.release(Locks.forDocumentEventsForEntities(entities));
        }
    }

    @Override
    public void markDocumentEventsProcessedOrFailed(Collection<? extends DocumentEvent> documentEvents,
            Collection<FailedDocumentEvent> failures) throws Exception {
        List<DocumentEventEntity> processed = documentEvents.stream()
                .map(LightblueEventRepository::asEntity)
                .collect(Collectors.toList());

        // TODO: Add field on document event entity to store error messages?
        List<DocumentEventEntity> failed = failures.stream()
                .map(FailedDocumentEvent::documentEvent)
                .map(LightblueEventRepository::asEntity)
                .collect(Collectors.toList());

        DataBulkRequest markDocumentEvents = new DataBulkRequest();
        markDocumentEvents.add(UpdateRequests.processingDocumentEventsAsProcessed(processed));
        markDocumentEvents.add(UpdateRequests.processingDocumentEventsAsFailed(failed));

        lightblue.data(markDocumentEvents);
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

    private static DocumentEventEntity asEntity(DocumentEvent event) {
        if (event instanceof LightblueDocumentEvent) {
            return ((LightblueDocumentEvent) event).wrappedDocumentEventEntity();
        }

        throw new EventHandlerException("Unknown event type. Only LightblueDocumentEvent are " +
                "supported. Event type was: " + event.getClass());
    }
}
