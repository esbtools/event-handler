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

import org.esbtools.eventhandler.DocumentEvent;
import org.esbtools.eventhandler.DocumentEventRepository;
import org.esbtools.eventhandler.EventHandlerException;
import org.esbtools.eventhandler.FailedDocumentEvent;
import org.esbtools.eventhandler.lightblue.model.DocumentEventEntity;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.model.Error;
import com.redhat.lightblue.client.request.DataBulkRequest;
import com.redhat.lightblue.client.response.LightblueBulkDataResponse;
import com.redhat.lightblue.client.response.LightblueBulkResponseException;
import com.redhat.lightblue.client.response.LightblueDataResponse;
import com.redhat.lightblue.client.response.LightblueErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
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
    private final LockStrategy lockStrategy;
    private final Map<String, ? extends DocumentEventFactory> documentEventFactoriesByType;
    private final Clock clock;

    private static final Logger logger = LoggerFactory.getLogger(LightblueDocumentEventRepository.class);

    public LightblueDocumentEventRepository(LightblueClient lightblue, String[] canonicalTypes,
            int documentEventBatchSize, LockStrategy lockStrategy,
            Map<String, ? extends DocumentEventFactory> documentEventFactoriesByType, Clock clock) {
        this.lightblue = lightblue;
        this.entities = canonicalTypes;
        this.documentEventBatchSize = documentEventBatchSize;
        this.documentEventFactoriesByType = documentEventFactoriesByType;
        this.clock = clock;
        this.lockStrategy = lockStrategy;
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
        // TODO: Also find expired processing
        DocumentEventEntity[] documentEventEntities = lightblue
                .data(FindRequests.priorityDocumentEventsForEntitiesUpTo(entities, documentEventBatchSize))
                .parseProcessed(DocumentEventEntity[].class);

        if (documentEventEntities.length == 0) {
            return Collections.emptyList();
        }

        Multimap<Identity, LightblueDocumentEvent> docEventsByIdentity = ArrayListMultimap.create();
        BulkLightblueRequester requester = new BulkLightblueRequester(lightblue);

        for (DocumentEventEntity eventEntity : documentEventEntities) {
            String typeOfEvent = eventEntity.getCanonicalType();

            DocumentEventFactory eventFactoryForType = documentEventFactoriesByType.get(typeOfEvent);

            if (eventFactoryForType == null) {
                // TODO: in dynamic config branch i have refactor that makes this impossible
                throw new NoSuchElementException("Document event factory not found for document " +
                        "event of type <" + typeOfEvent + ">. Entity looks like: " + eventEntity);
            }

            // TODO: catch failures here, fail only events which failed to parse
            LightblueDocumentEvent newEvent =
                    eventFactoryForType.getDocumentEventForEntity(eventEntity, requester);

            docEventsByIdentity.put(newEvent.identity(), newEvent);
        }

        try (LockedResources<Identity> lock =
                     lockStrategy.tryAcquireUpTo(maxEvents, docEventsByIdentity.keySet())) {
            List<Identity> lockedIdentities = lock.getResources();

            List<Identity> retrievedIdentities = new ArrayList<>(docEventsByIdentity.keySet());

            for (Identity retrievedIdentity : retrievedIdentities) {
                if (!lockedIdentities.contains(retrievedIdentity)) {
                    docEventsByIdentity.removeAll(retrievedIdentity);
                } else {
                    optimizeDocumentEventsUpTo(docEventsByIdentity.get(retrievedIdentity));
                }
            }

            List<LightblueDocumentEvent> updated =
                    persistNewEntitiesAndStatusUpdatesToExisting(docEventsByIdentity, lock);

            return updated.stream()
                    .sorted((o1, o2) -> o2.wrappedDocumentEventEntity().getPriority() -
                            o1.wrappedDocumentEventEntity().getPriority())
                    .filter(event -> event.wrappedDocumentEventEntity().getStatus()
                            .equals(DocumentEventEntity.Status.processing))
                    .collect(Collectors.toList());
        }
    }

    @Override
    public List<? extends DocumentEvent> filterExpired(List<? extends DocumentEvent> events) {
        List<LightblueDocumentEvent> filtered = new ArrayList<>(events.size());

        for (DocumentEvent event : events) {
            if (!(event instanceof LightblueDocumentEvent)) {
                throw new IllegalArgumentException("Unknown event type. Only " +
                        "LightblueDocumentEvent is supported. Event type was: " +
                        event.getClass());
            }

            LightblueDocumentEvent lightblueEvent = (LightblueDocumentEvent) event;

            ZonedDateTime processingDate = lightblueEvent.wrappedDocumentEventEntity().getProcessingDate();
            ZonedDateTime expireDate = processingDate.plusMinutes(5);

            if (clock.instant().isBefore(expireDate.toInstant())) {
                filtered.add(lightblueEvent);
            }
        }

        return filtered;
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
     * @param events The priority-first parsed events from lightblue, modified in place.
     *               TODO: update this javadoc
     * @return An optimized list of parsed or merged document events ready to be published. No more
     * than {@code maxEvents} will be returned. List may include events with newly computed entities
     * that are not yet persisted.
     */
    private void optimizeDocumentEventsUpTo(Collection<LightblueDocumentEvent> events) {
        // TODO: This algorithm be able to be optimized more now that we are modifying the collection
        // in place and we know all events have same identity.

        List<LightblueDocumentEvent> newEvents = new ArrayList<>(events);
        List<LightblueDocumentEvent> optimized = new ArrayList<>();

        for (final LightblueDocumentEvent newEvent : newEvents) {
            // We have a new event, let's see if it is superseded by or can be merged with any
            // previous events we parsed or created as a result of a previous merge.

            // As we check, if we find we can merge an event, we will merge it, and continue on with
            // the merger instead. These pointers track which event we are currently optimizing.
            @Nullable LightblueDocumentEvent newOrMergerEvent = newEvent;
            DocumentEventEntity newOrMergerEventEntity = newEvent.wrappedDocumentEventEntity();

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

            if (newOrMergerEvent != null) {
                newOrMergerEventEntity.setStatus(DocumentEventEntity.Status.processing);
                optimized.add(newOrMergerEvent);

                if (!events.contains(newOrMergerEvent)) {
                    events.add(newOrMergerEvent);
                }
            }
        }
    }

    /**
     * Updates event status, processing date, and survivor ids for given event entities. Persists
     * new event entities among document event list, and updates entities for those events with
     * persisted ids.
     *
     * TODO: update this javadoc
     *
     * @throws LightblueException
     */
    private List<LightblueDocumentEvent> persistNewEntitiesAndStatusUpdatesToExisting(
            Multimap<Identity, LightblueDocumentEvent> docEventsByIdentity,
            LockedResources<Identity> identityLocks) throws LightblueException, LostLockException {
        DataBulkRequest insertAndUpdateEvents = new DataBulkRequest();

        for (Identity lostIdentity : identityLocks.checkForLostResources()) {
            logger.warn("Lost lock for event identity: {}", lostIdentity);
            docEventsByIdentity.removeAll(lostIdentity);
        }

        if (docEventsByIdentity.isEmpty()) {
            return Collections.emptyList();
        }

        List<LightblueDocumentEvent> eventsAsList = new ArrayList<>(docEventsByIdentity.values());

        // TODO: We make single request per event here (wrapped in bulk request). Could optimize.
        // See: https://github.com/esbtools/event-handler/issues/11
        for (LightblueDocumentEvent event : eventsAsList) {
            DocumentEventEntity entity = event.wrappedDocumentEventEntity();

            if (entity.get_id() == null && entity.getStatus().equals(DocumentEventEntity.Status.processing)) {
                insertAndUpdateEvents.add(InsertRequests.documentEventsReturningOnlyIds(entity));
            } else {
                ZonedDateTime processingTime = ZonedDateTime.now(clock);
                insertAndUpdateEvents.add(
                        UpdateRequests.documentEventAsProcessingIfCurrent(entity, processingTime));
            }
        }

        LightblueBulkDataResponse bulkResponse;

        try {
            bulkResponse = lightblue.bulkData(insertAndUpdateEvents);
        } catch (LightblueBulkResponseException e) {
            // If some failed, that's okay. We have to iterate through responses either way.
            // We'll check for errors then.
            bulkResponse = e.getBulkResponse();
        }

        Iterator<LightblueDocumentEvent> eventsIterator = eventsAsList.iterator();
        Iterator<LightblueDataResponse> responsesIterator = bulkResponse.getResponses().iterator();

        while (eventsIterator.hasNext()) {
            if (!responsesIterator.hasNext()) {
                throw new IllegalStateException("Mismatched number of requests and responses! " +
                        "Events looked like: <{}>. Responses looked like");
            }

            LightblueDataResponse response = responsesIterator.next();
            LightblueDocumentEvent event = eventsIterator.next();
            DocumentEventEntity entity = event.wrappedDocumentEventEntity();

            if (response instanceof LightblueErrorResponse) {
                LightblueErrorResponse errorResponse = (LightblueErrorResponse) response;

                // Likely transient failure; leave event alone to be tried again later.
                if (errorResponse.hasDataErrors() || errorResponse.hasLightblueErrors()) {
                    if (logger.isWarnEnabled()) {
                        List<Error> errors = new ArrayList<>();

                        Collections.addAll(errors, errorResponse.getLightblueErrors());
                        Collections.addAll(errors, Arrays.stream(errorResponse.getDataErrors())
                                .flatMap(dataError -> dataError.getErrors().stream())
                                .toArray(Error[]::new));

                        List<String> errorStrings = errors.stream()
                                .map(e -> "Code: " + e.getErrorCode() + ", " +
                                        "Context: " + e.getContext() + ", " +
                                        "Message: " + e.getMsg())
                                .collect(Collectors.toList());

                        logger.warn("Event update failed. Will not process. Event was: <{}>. " +
                                "Errors: <{}>", event, errorStrings);
                    }

                    eventsIterator.remove();

                    continue;
                }
            }

            if (response.parseModifiedCount() == 0) {
                logger.warn("Event updated by another thread. Will not process. Event was: {}", event);

                eventsIterator.remove();

                continue;
            }

            // If known entity has no id, must've been insert. Populate id in returned entity.
            if (entity.get_id() == null) {
                DocumentEventEntity processed = response.parseProcessed(DocumentEventEntity.class);
                entity.set_id(processed.get_id());
            }
        }

        return eventsAsList;
    }

    private static DocumentEventEntity asEntity(DocumentEvent event) {
        if (event instanceof LightblueDocumentEvent) {
            return ((LightblueDocumentEvent) event).wrappedDocumentEventEntity();
        }

        throw new EventHandlerException("Unknown event type. Only LightblueDocumentEvent is " +
                "supported. Event type was: " + event.getClass());
    }
}
