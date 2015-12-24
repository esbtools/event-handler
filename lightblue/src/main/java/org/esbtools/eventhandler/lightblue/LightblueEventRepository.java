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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;

// TODO: Doesn't have to be same class for this
public class LightblueEventRepository implements EventRepository, NotificationRepository {
    private final LightblueClient lightblue;
    private final String[] entities;
    private final int documentEventBatchSize;
    private final Locking locking;
    private final NotificationFactory notificationFactory;
    private final DocumentEventFactory documentEventFactory;

    public LightblueEventRepository(LightblueClient lightblue, String[] entities,
            int documentEventBatchSize, String lockingDomain,
            NotificationFactory notificationFactory, DocumentEventFactory documentEventFactory) {
        this.lightblue = lightblue;
        this.entities = entities;
        this.documentEventBatchSize = documentEventBatchSize;
        this.notificationFactory = notificationFactory;
        this.documentEventFactory = documentEventFactory;

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
                    .data(Find.newNotificationsForEntitiesUpTo(entities, maxEvents))
                    .parseProcessed(NotificationEntity[].class);

            lightblue.data(Update.notificationsAsProcessing(notificationEntities));

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
                .map(LightblueEventRepository::toWrappedNotificationEntity)
                .collect(Collectors.toList());

        // TODO: Add field in NotificationEntity for failure messages?
        List<NotificationEntity> failedNotificationEntities = failures.stream()
                .map(FailedNotification::notification)
                .map(LightblueEventRepository::toWrappedNotificationEntity)
                .collect(Collectors.toList());

        DataBulkRequest markNotifications = new DataBulkRequest();
        markNotifications.add(Update.processingNotificationsAsProcessed(processedNotificationEntities));
        markNotifications.add(Update.processingNotificationsAsFailed(failedNotificationEntities));

        lightblue.bulkData(markNotifications);
    }

    @Override
    public void addNewDocumentEvents(Collection<DocumentEvent> documentEvents)
            throws LightblueException {
        List<DocumentEventEntity> documentEventEntities = new ArrayList<>(documentEvents.size());

        for (DocumentEvent documentEvent : documentEvents) {
            documentEventEntities.add(toNewDocumentEventEntity(documentEvent));
        }

        lightblue.data(Insert.documentEvents(documentEventEntities));
    }

    @Override
    public List<DocumentEvent> retrievePriorityDocumentEventsUpTo(int maxEvents)
            throws LightblueException {
        try {
            blockUntilLockAcquired(Locks.forDocumentEventsForEntities(entities));

            DocumentEventEntity[] documentEventEntities = lightblue
                    .data(Find.priorityDocumentEventsForEntitiesUpTo(entities, documentEventBatchSize))
                    .parseProcessed(DocumentEventEntity[].class);

            List<DocumentEvent> optimized = new ArrayList<>(maxEvents);
            List<DocumentEventEntity> mergerEntities = new ArrayList<>();
            List<DocumentEventEntity> entitiesToUpdate = new ArrayList<>();
            BulkLightblueRequester requester = new BulkLightblueRequester(lightblue);

            for (DocumentEventEntity newEventEntity : documentEventEntities) {
                newEventEntity.setStatus(DocumentEventEntity.Status.processing);

                entitiesToUpdate.add(newEventEntity);

                DocumentEvent newEvent = documentEventFactory.getDocumentEventForEntity(newEventEntity, requester);

                // If there is a merge out of this event, it will need to be inserted later, since
                // it is a net new event.
                Optional<DocumentEventEntity> maybeMergerEntity = Optional.empty();

                Iterator<DocumentEvent> previousEventIterator = optimized.iterator();
                while (previousEventIterator.hasNext()) {
                    DocumentEvent previousEvent = previousEventIterator.next();

                    if (newEvent.isSupersededBy(previousEvent)) {
                        DocumentEventEntity previousEntity = toWrappedDocumentEventEntity(previousEvent);
                        newEventEntity.setStatus(DocumentEventEntity.Status.superseded);
                        newEventEntity.setSurvivedById(previousEntity.get_id());
                        newEvent = null;
                        break;
                    } else if (newEvent.couldMergeWith(previousEvent)) {
                        DocumentEvent merger = newEvent.merge(previousEvent);
                        DocumentEventEntity mergerEntity = toWrappedDocumentEventEntity(merger);
                        mergerEntity.setStatus(DocumentEventEntity.Status.processing);

                        DocumentEventEntity previousEntity = toWrappedDocumentEventEntity(previousEvent);
                        previousEntity.setStatus(DocumentEventEntity.Status.merged);
                        previousEntity.setSurvivedById(mergerEntity.get_id());

                        newEventEntity.setStatus(DocumentEventEntity.Status.merged);
                        newEventEntity.setSurvivedById(mergerEntity.get_id());

                        newEvent = merger;
                        maybeMergerEntity = Optional.of(mergerEntity);
                        previousEventIterator.remove();
                    }
                }

                if (newEvent != null) {
                    optimized.add(newEvent);
                    maybeMergerEntity.ifPresent(mergerEntities::add);
                }

                // TODO: This could be optimized a little better but will improve this soon after tests
                if (optimized.size() >= maxEvents) {
                    break;
                }
            }

            DataBulkRequest insertAndUpdateEvents = new DataBulkRequest();
            if (!mergerEntities.isEmpty()) {
                insertAndUpdateEvents.add(Insert.documentEvents(mergerEntities));
            }

            insertAndUpdateEvents.addAll(Update.newDocumentEventsStatusAndSurvivedBy(entitiesToUpdate));

            if (insertAndUpdateEvents.getRequests().size() == 1) {
                lightblue.data(insertAndUpdateEvents.getRequests().get(0));
            } else {
                lightblue.data(insertAndUpdateEvents);
            }

            return optimized;
        } finally {
            locking.release(Locks.forDocumentEventsForEntities(entities));
        }
    }

    @Override
    public void markDocumentEventsProcessedOrFailed(Collection<DocumentEvent> documentEvents,
            Collection<FailedDocumentEvent> failures) throws Exception {
        List<DocumentEventEntity> processed = documentEvents.stream()
                .map(LightblueEventRepository::toWrappedDocumentEventEntity)
                .collect(Collectors.toList());

        // TODO: Add field on document event entity to store error messages?
        List<DocumentEventEntity> failed = failures.stream()
                .map(FailedDocumentEvent::documentEvent)
                .map(LightblueEventRepository::toWrappedDocumentEventEntity)
                .collect(Collectors.toList());

        DataBulkRequest markDocumentEvents = new DataBulkRequest();
        markDocumentEvents.add(Update.processingDocumentEventsAsProcessed(processed));
        markDocumentEvents.add(Update.processingDocumentEventsAsFailed(failed));

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

    private static NotificationEntity toWrappedNotificationEntity(Notification notification) {
        if (notification instanceof LightblueNotification) {
            return ((LightblueNotification) notification).wrappedNotificationEntity();
        }

        throw new EventHandlerException("Unknown notification type. Only LightblueNotification " +
                "are supported. Event type was: " + notification.getClass());
    }

    private static DocumentEventEntity toWrappedDocumentEventEntity(DocumentEvent event) {
        if (event instanceof LightblueDocumentEvent) {
            return ((LightblueDocumentEvent) event).wrappedDocumentEventEntity().orElseThrow(() ->
                    new NoSuchElementException("Expected event to be backed by existing document " +
                            "event entity, but was: " + event));
        }

        throw new EventHandlerException("Unknown event type. Only LightblueDocumentEvent are " +
                "supported. Event type was: " + event.getClass());
    }

    private static DocumentEventEntity toNewDocumentEventEntity(DocumentEvent event) {
        if (event instanceof LightblueDocumentEvent) {
            return ((LightblueDocumentEvent) event).toNewDocumentEventEntity();
        }

        throw new EventHandlerException("Unknown event type. Only LightblueDocumentEvent are " +
                "supported. Event type was: " + event.getClass());
    }
}
