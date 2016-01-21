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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LightblueDocumentEventRepository implements DocumentEventRepository {
    private final LightblueClient lightblue;
    private final LightblueDocumentEventRepositoryConfig config;
    private final LockStrategy lockStrategy;
    private final Map<String, ? extends DocumentEventFactory> documentEventFactoriesByType;
    private final Clock clock;

    private final Set<String> supportedTypes;
    /** Cached to avoid extra garbage. */
    private final String[] supportedTypesArray;

    private static final Logger logger = LoggerFactory.getLogger(LightblueDocumentEventRepository.class);

    public LightblueDocumentEventRepository(LightblueClient lightblue,
            LockStrategy lockStrategy, LightblueDocumentEventRepositoryConfig config,
            Map<String, ? extends DocumentEventFactory> documentEventFactoriesByType, Clock clock) {
        this.lightblue = lightblue;
        this.lockStrategy = lockStrategy;
        this.config = config;
        this.documentEventFactoriesByType = documentEventFactoriesByType;
        this.clock = clock;

        supportedTypes = documentEventFactoriesByType.keySet();
        supportedTypesArray = supportedTypes.toArray(new String[supportedTypes.size()]);
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
        String[] typesToProcess = getSupportedAndEnabledEventTypes();
        int documentEventsBatchSize = config.getDocumentEventsBatchSize();

        if (typesToProcess.length == 0 || documentEventsBatchSize == 0) {
            return Collections.emptyList();
        }

        try (LockedResource lock = lockStrategy
                .blockUntilAcquired(ResourceIds.forDocumentEventsForTypes(typesToProcess))) {
            DocumentEventEntity[] documentEventEntities = lightblue
                    .data(FindRequests.priorityDocumentEventsForTypesUpTo(typesToProcess, documentEventsBatchSize))
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

    private String[] getSupportedAndEnabledEventTypes() {
        Set<String> canonicalTypesToProcess = config.getCanonicalTypesToProcess();

        if (canonicalTypesToProcess.containsAll(supportedTypes)) {
            return supportedTypesArray;
        }

        List<String> supportedAndEnabled = new ArrayList<>(supportedTypes);
        supportedAndEnabled.retainAll(canonicalTypesToProcess);
        return supportedAndEnabled.toArray(new String[supportedAndEnabled.size()]);
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
     * <p>Events for canonical types not supported will log warnings. Event processing will not be
     * interrupted. If this happens it is a bug, however.
     *
     * @param maxEvents The maximum number of parsed document events to return. Not to be confused
     *                  with {@link #config}'s
     *                  {@link LightblueDocumentEventRepositoryConfig#getDocumentEventsBatchSize()
     *                  documentEventsBatchSize}.
     * @param documentEventEntities The priority-first entities from lightblue.
     * @param entitiesToUpdate A mutable collection of entities who's status modifications should be
     *                         persisted back into lightblue. This gets populated by the
     *                         optimization process.
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
                logger.warn("No document event factory configured for document event of type '{}'." +
                        "Entity looks like: {}", typeOfEvent, newEventEntity);
                continue;
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
     * @param maybeNewEvents List of document events which may have yet-to-be-persisted entities.
     *         these entities will be persisted and ids retrieved to mutate the events in this list
     *         list with those ids.
     * @throws LightblueException
     */
    private void persistNewEntitiesAndStatusUpdatesToExisting(
            List<DocumentEventEntity> entitiesToUpdate,
            List<LightblueDocumentEvent> maybeNewEvents,
            LockedResource requiredLock) throws LightblueException, LostLockException {
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

        requiredLock.ensureAcquiredOrThrow("Will not process found document events.");

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
