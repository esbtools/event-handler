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
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.request.DataBulkRequest;
import com.redhat.lightblue.client.response.LightblueBulkDataResponse;
import com.redhat.lightblue.client.response.LightblueDataResponse;

import org.esbtools.eventhandler.DocumentEvent;
import org.esbtools.eventhandler.DocumentEventRepository;
import org.esbtools.eventhandler.EventHandlerException;
import org.esbtools.eventhandler.FailedDocumentEvent;
import org.esbtools.eventhandler.lightblue.model.DocumentEventEntity;

import javax.annotation.Nullable;
import java.time.Clock;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class LightblueDocumentEventRepository implements DocumentEventRepository {
    private final LightblueClient lightblue;
    private final String[] entities;

    /**
     * The amount of document events to sort through for optimizations at one time.
     *
     * <p>This should be much larger than that actual number of events you want to publish at one
     * time due to optimizations that reduce the actual number of publishable events. For example,
     * in a batch of 100 events, if 50 of them are redundant or able to be merged, you'll end up
     * with only 50 discrete events. If you only grabbed 50, you might miss some of those potential
     * optimizations.
     *
     * <p>Events not included to be published among the batch (not including events that were merged
     * or superseded), should be put back in the document event entity pool to be processed later.
     */
    private final int documentEventBatchSize;
    private final AutoLocker locker;
    private final Map<String, ? extends DocumentEventFactory> documentEventFactoriesByType;
    private final Clock clock;;

    public LightblueDocumentEventRepository(LightblueClient lightblue, String[] canonicalTypes,
            int documentEventBatchSize, String lockingDomain,
            Map<String, ? extends DocumentEventFactory> documentEventFactoriesByType,
            Duration lockRefresh, Clock clock) {
        this.lightblue = lightblue;
        this.entities = canonicalTypes;
        this.documentEventBatchSize = documentEventBatchSize;
        this.documentEventFactoriesByType = documentEventFactoriesByType;
        this.clock = clock;

        locker = new AutoLocker(lightblue.getLocking(lockingDomain), Duration.ofSeconds(3));
    }

    @Override
    public void addNewDocumentEvents(Collection<? extends DocumentEvent> documentEvents)
            throws LightblueException {
        if (documentEvents.isEmpty()) {
            return;
        }

        List<DocumentEventEntity> documentEventEntities = documentEvents.stream()
                .map(LightblueDocumentEventRepository::asEntity)
                .collect(Collectors.toList());

        lightblue.data(InsertRequests.documentEventsReturningOnlyIds(documentEventEntities));
    }

    @Override
    public List<LightblueDocumentEvent> retrievePriorityDocumentEventsUpTo(int maxEvents)
            throws Exception {
        try (LightblueLock lock = locker.blockUntilAcquiredPingingEvery(
                Duration.ofSeconds(5), ResourceIds.forDocumentEventsForEntities(entities))) {
            DocumentEventEntity[] documentEventEntities = lightblue
                    .data(FindRequests.priorityDocumentEventsForEntitiesUpTo(entities, documentEventBatchSize))
                    .parseProcessed(DocumentEventEntity[].class);

            if (documentEventEntities.length == 0) {
                return Collections.emptyList();
            }

            List<DocumentEventEntity> entitiesToUpdate = new ArrayList<>();
            BulkLightblueRequester requester = new BulkLightblueRequester(lightblue);

            List<LightblueDocumentEvent> optimized = parseAndOptimizeDocumentEventEntitiesUpTo(
                    maxEvents, documentEventEntities, entitiesToUpdate, requester);

            persistNewEntitiesAndStatusUpdatesToExisting(entitiesToUpdate, optimized, lock);

            return optimized;
        }
    }

    @Override
    public void markDocumentEventsProcessedOrFailed(
            Collection<? extends DocumentEvent> documentEvents,
            Collection<FailedDocumentEvent> failures) throws LightblueException {
        List<DocumentEventEntity> processed = documentEvents.stream()
                .map(LightblueDocumentEventRepository::asEntity)
                .peek((e) -> {
                    e.setProcessedDate(ZonedDateTime.now(clock));
                    e.setStatus(DocumentEventEntity.Status.published);
                })
                .collect(Collectors.toList());

        List<DocumentEventEntity> failed = failures.stream()
                .map(FailedDocumentEvent::documentEvent)
                .map(LightblueDocumentEventRepository::asEntity)
                .peek((e) -> {
                    e.setProcessedDate(ZonedDateTime.now(clock));
                    e.setStatus(DocumentEventEntity.Status.failed);
                })
                .collect(Collectors.toList());

        DataBulkRequest markDocumentEvents = new DataBulkRequest();
        markDocumentEvents.addAll(UpdateRequests.documentEventsStatusAndProcessedDate(processed));
        markDocumentEvents.addAll(UpdateRequests.documentEventsStatusAndProcessedDate(failed));

        if (markDocumentEvents.getRequests().isEmpty()) {
            return;
        }

        // TODO: What if some of these fail?
        // Documents are published, so not world ending, but important.
        // Waiting on: https://github.com/lightblue-platform/lightblue-client/issues/202
        lightblue.bulkData(markDocumentEvents);
    }

    /**
     * Creates {@link LightblueDocumentEvent}s from entities using
     * {@link #documentEventFactoriesByType}, optimizes away superseded and merge-able events, and
     * populates {@code entitiesToUpdate} with entity status updates that should be persisted to the
     * document event entity collection before releasing locks.
     *
     * <p>The returned list may include net-new events as the result of merges. These events do not
     * yet have an associated <em>persisted</em> entity, and therefore have no id's.
     *
     * @param maxEvents The maximum number of parsed document events to return. Not to be confused
     *                  with {@link #documentEventBatchSize}.
     * @param documentEventEntities The priority-first entities from lightblue.
     * @param entitiesToUpdate A mutable collection of entities who's status modifications should be
     *                         persisted back into lightblue.
     * @param requester Used by {@link LightblueDocumentEvent} implementations to lookup their
     *                  corresponding documents.
     * @return An optimized list of parsed or merged document events ready to be published. No more
     * than {@code maxEvents} will be returned. List may include events with newly computed entities
     * that are not yet persisted.
     */
    private List<LightblueDocumentEvent> parseAndOptimizeDocumentEventEntitiesUpTo(int maxEvents,
            DocumentEventEntity[] documentEventEntities, List<DocumentEventEntity> entitiesToUpdate,
            LightblueRequester requester) {
        List<LightblueDocumentEvent> optimized = new ArrayList<>(maxEvents);

        for (final DocumentEventEntity newEventEntity : documentEventEntities) {
            String typeOfEvent = newEventEntity.getCanonicalType();

            DocumentEventFactory eventFactoryForType = documentEventFactoriesByType.get(typeOfEvent);

            if (eventFactoryForType == null) {
                throw new NoSuchElementException("Document event factory not found for document " +
                        "event of type <" + typeOfEvent + ">. Entity looks like: " + newEventEntity);
            }

            final LightblueDocumentEvent newEvent =
                    eventFactoryForType.getDocumentEventForEntity(newEventEntity, requester);

            // We have a new event, let's see if it is superseded by or can be merged with any
            // previous events we parsed or created as a result of a previous merge.

            // As we check, if we find we can merge an event, we will merge it, and continue on with
            // the merger instead. These pointers track which event we are currently optimizing.
            @Nullable DocumentEventEntity newOrMergerEventEntity = newEventEntity;
            @Nullable LightblueDocumentEvent newOrMergerEvent = newEvent;

            Iterator<LightblueDocumentEvent> optimizedIterator = optimized.iterator();

            while (optimizedIterator.hasNext()) {
                LightblueDocumentEvent previouslyOptimizedEvent = optimizedIterator.next();

                if (newOrMergerEvent.isSupersededBy(previouslyOptimizedEvent)) {
                    // Keep previous event...
                    DocumentEventEntity previousEntity = previouslyOptimizedEvent.wrappedDocumentEventEntity();
                    previousEntity.addSurvivorOfIds(newOrMergerEventEntity.getSurvivorOfIds());
                    previousEntity.addSurvivorOfIds(newOrMergerEventEntity.get_id());

                    // ...and throw away this new one.
                    newOrMergerEventEntity.setStatus(DocumentEventEntity.Status.superseded);
                    newOrMergerEventEntity.setProcessedDate(ZonedDateTime.now(clock));
                    entitiesToUpdate.add(newOrMergerEventEntity);

                    newOrMergerEvent = null;
                    break;
                } else if (newOrMergerEvent.couldMergeWith(previouslyOptimizedEvent)) {
                    // Previous entity was processing; now it is merged and removed from optimized
                    // result list.
                    DocumentEventEntity previousEntity = previouslyOptimizedEvent.wrappedDocumentEventEntity();
                    previousEntity.setStatus(DocumentEventEntity.Status.merged);
                    previousEntity.setProcessedDate(ZonedDateTime.now(clock));
                    optimizedIterator.remove();

                    // This new event will not be included in result list, but we do have to update
                    // its entity to store that it has been merged.
                    newOrMergerEventEntity.setStatus(DocumentEventEntity.Status.merged);
                    newOrMergerEventEntity.setProcessedDate(ZonedDateTime.now(clock));
                    entitiesToUpdate.add(newOrMergerEventEntity);

                    // We create a new event as a result of the merger, and keep this instead of the
                    // others.
                    LightblueDocumentEvent merger = newOrMergerEvent.merge(previouslyOptimizedEvent);
                    DocumentEventEntity mergerEntity = merger.wrappedDocumentEventEntity();
                    mergerEntity.addSurvivorOfIds(previousEntity.getSurvivorOfIds());
                    mergerEntity.addSurvivorOfIds(newOrMergerEventEntity.getSurvivorOfIds());
                    if (previousEntity.get_id() != null) {
                        mergerEntity.addSurvivorOfIds(previousEntity.get_id());
                    }
                    if (newOrMergerEventEntity.get_id() != null) {
                        mergerEntity.addSurvivorOfIds(newOrMergerEventEntity.get_id());
                    }

                    newOrMergerEvent = merger;
                    newOrMergerEventEntity = mergerEntity;
                }
            }

            // Only add the event if we've not hit our limit. We should keep going however even if
            // we have hit our limit, because some of the remaining events may be superseded or
            // merged as a part of the current optimized batch.
            if (newOrMergerEvent != null && optimized.size() < maxEvents) {
                newOrMergerEventEntity.setStatus(DocumentEventEntity.Status.processing);
                entitiesToUpdate.add(newOrMergerEventEntity);
                optimized.add(newOrMergerEvent);
            }
        }

        return optimized;
    }

    /**
     * Updates event status, processing date, and survivor ids for given event entities. Persists
     * new event entities among document event list, and updates entities for those events with
     * persisted ids.
     *
     * @param entitiesToUpdate List of existing entities which have status updates.
     * @param maybeNewEvents List of document events which may have yet-to-be-persisted entities. These
     *                  entities will be persisted and ids retrieved to mutate the events in this
     *                  list with those ids.
     * @throws LightblueException
     */
    private void persistNewEntitiesAndStatusUpdatesToExisting(
            List<DocumentEventEntity> entitiesToUpdate,
            List<LightblueDocumentEvent> maybeNewEvents,
            LightblueLock requiredLock) throws Exception {
        DataBulkRequest insertAndUpdateEvents = new DataBulkRequest();
        List<LightblueDocumentEvent> newEvents = new ArrayList<>();

        // Add requests to plop in new entities in order.
        for (LightblueDocumentEvent event : maybeNewEvents) {
            DocumentEventEntity entity = event.wrappedDocumentEventEntity();
            if (entity.get_id() == null) {
                newEvents.add(event);
                insertAndUpdateEvents.add(InsertRequests.documentEventsReturningOnlyIds(entity));
            }
        }

        insertAndUpdateEvents.addAll(UpdateRequests.documentEventsStatusAndProcessedDate(entitiesToUpdate));

        if (!requiredLock.ping()) {
            throw new LostLockException(requiredLock, "Will not process found document events.");
        }

        // TODO: Verify these were all successful
        LightblueBulkDataResponse bulkResponse = lightblue.bulkData(insertAndUpdateEvents);
        List<LightblueDataResponse> responses = bulkResponse.getResponses();

        // Read responses for new entities in order, update corresponding events' entities with
        // new ids.
        for (int i = 0; i < newEvents.size(); i++) {
            LightblueDataResponse response = responses.get(i);
            LightblueDocumentEvent newEvent = newEvents.get(i);
            DocumentEventEntity newEntity = response.parseProcessed(DocumentEventEntity.class);
            newEvent.wrappedDocumentEventEntity().set_id(newEntity.get_id());
        }
    }

    private static DocumentEventEntity asEntity(DocumentEvent event) {
        if (event instanceof LightblueDocumentEvent) {
            return ((LightblueDocumentEvent) event).wrappedDocumentEventEntity();
        }

        throw new EventHandlerException("Unknown event type. Only LightblueDocumentEvent are " +
                "supported. Event type was: " + event.getClass());
    }
}
