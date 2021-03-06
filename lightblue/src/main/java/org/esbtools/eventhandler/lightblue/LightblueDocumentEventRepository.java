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
import org.esbtools.eventhandler.FailedDocumentEvent;
import org.esbtools.eventhandler.lightblue.client.BulkLightblueRequester;
import org.esbtools.eventhandler.lightblue.client.FindRequests;
import org.esbtools.eventhandler.lightblue.client.InsertRequests;
import org.esbtools.eventhandler.lightblue.client.LightblueErrors;
import org.esbtools.eventhandler.lightblue.client.LightblueRequester;
import org.esbtools.eventhandler.lightblue.client.UpdateRequests;
import org.esbtools.eventhandler.lightblue.locking.LockNotAvailableException;
import org.esbtools.eventhandler.lightblue.locking.LockStrategy;
import org.esbtools.eventhandler.lightblue.locking.Lockable;
import org.esbtools.eventhandler.lightblue.locking.LockedResource;
import org.esbtools.eventhandler.lightblue.locking.LockedResources;
import org.esbtools.eventhandler.lightblue.locking.LostLockException;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.request.DataBulkRequest;
import com.redhat.lightblue.client.response.LightblueBulkDataResponse;
import com.redhat.lightblue.client.response.LightblueBulkResponseException;
import com.redhat.lightblue.client.response.LightblueDataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A document event repository which uses lightblue as the event store, storing events in the form
 * of {@link DocumentEventEntity}, which must be configured as an entity in lightblue.
 */
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

        int newEventsCount = documentEventEntities.size();
        int maxEventsPerInsert = config.getOptionalMaxDocumentEventsPerInsert().orElse(newEventsCount);

        int insertCount = (int) Math.ceil((double) newEventsCount / maxEventsPerInsert);

        for (int i = 0; i < insertCount; i++) {
            int fromIndex = i * maxEventsPerInsert;
            int toIndex = Math.min(fromIndex + maxEventsPerInsert, newEventsCount);

            List<DocumentEventEntity> entitiesInBatch =
                    documentEventEntities.subList(fromIndex, toIndex);

            logger.debug("Inserting batch #{} of new document events from {} to {}.",
                    i, fromIndex, toIndex);

            lightblue.data(InsertRequests.documentEventsReturningOnlyIds(entitiesInBatch));
        }
    }

    @Override
    public List<LightblueDocumentEvent> retrievePriorityDocumentEventsUpTo(int maxEvents)
            throws Exception {
        String[] typesToProcess = getSupportedAndEnabledEventTypes();
        Integer documentEventsBatchSize = config.getDocumentEventsBatchSize();
        Duration processingTimeout = config.getDocumentEventProcessingTimeout();

        if (typesToProcess.length == 0 || documentEventsBatchSize == null ||
                documentEventsBatchSize == 0) {
            logger.info("Not retrieving any document events because either there are no enabled " +
                    "or supported types to process or documentEventBatchSize is 0. Supported " +
                    "types are {}. Of those, enabled types are {}. " +
                    "Document event batch size is {}.",
                    supportedTypes, Arrays.toString(typesToProcess), documentEventsBatchSize);
            return Collections.emptyList();
        }

        if (maxEvents == 0) {
            return Collections.emptyList();
        }

        DocumentEventEntity[] documentEventEntities = lightblue
                .data(FindRequests.priorityDocumentEventsForTypesUpTo(
                        typesToProcess, documentEventsBatchSize,
                        clock.instant().minus(processingTimeout)))
                .parseProcessed(DocumentEventEntity[].class);

        if (documentEventEntities.length == 0) {
            return Collections.emptyList();
        }

        try (LockedResources<SharedIdentityEvents> eventLocks =
                     SharedIdentityEvents.parseAndOptimizeLockableDocumentEventEntities(
                             maxEvents,
                             documentEventEntities,
                             new BulkLightblueRequester(lightblue),
                             documentEventFactoriesByType,
                             lockStrategy,
                             clock)) {

            return persistNewEventsAndStatusUpdatesToExisting(eventLocks);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>N.B. This implementation currently works by simply checking if the known in memory
     * timeouts of provided event falls within some threshold. It could be updated to also timestamp
     * the persisted event again, but to be safe we would probably want to also lock them to do
     * this update. For now, that complexity is probably not worth it.
     */
    @Override
    public void ensureTransactionActive(DocumentEvent event) throws Exception {
        if (!(event instanceof LightblueDocumentEvent)) {
            throw new IllegalArgumentException("Unknown event type. Only LightblueDocumentEvent " +
                    "is supported. Event type was: " + event.getClass());
        }

        LightblueDocumentEvent lightblueEvent = (LightblueDocumentEvent) event;
        Duration processingTimeout = config.getDocumentEventProcessingTimeout();
        Duration expireThreshold = config.getDocumentEventExpireThreshold();

        Instant processingDate = lightblueEvent.wrappedDocumentEventEntity()
                .getProcessingDate().toInstant();
        Instant expireDate = processingDate.plus(processingTimeout).minus(expireThreshold);

        if (clock.instant().isAfter(expireDate)) {
            throw new ProcessingExpiredException(event, processingTimeout, expireThreshold);
        }
    }

    @Override
    public void markDocumentEventsPublishedOrFailed(
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

        // TODO: Check if only one request, then don't do bulk.

        // If any fail, not much we can do. Let exception propagate.
        lightblue.bulkData(markDocumentEvents);
    }

    private String[] getSupportedAndEnabledEventTypes() {
        Set<String> canonicalTypesToProcess = config.getCanonicalTypesToProcess();

        if (canonicalTypesToProcess == null) {
            return new String[0];
        }

        if (canonicalTypesToProcess.containsAll(supportedTypes)) {
            return supportedTypesArray;
        }

        List<String> supportedAndEnabled = new ArrayList<>(supportedTypes);
        supportedAndEnabled.retainAll(canonicalTypesToProcess);
        return supportedAndEnabled.toArray(new String[supportedAndEnabled.size()]);
    }

    /**
     * Within locked events, inserts new entities and updates existing with status, date, and
     * survivor id updates.
     *
     * <p>Checks for lost locks before persisting, dropping and logging those lost.
     */
    private List<LightblueDocumentEvent> persistNewEventsAndStatusUpdatesToExisting(
            LockedResources<SharedIdentityEvents> identityLocks) throws LightblueException {
        if (identityLocks.getLocks().isEmpty()) {
            return Collections.emptyList();
        }

        DataBulkRequest insertAndUpdateEvents = new DataBulkRequest();
        List<LightblueDocumentEvent> savedEvents = new ArrayList<>();

        // TODO: We make single request per event here (wrapped in bulk request). Maybe could optimize.
        // Right now each event may have different processing date which we are looking for.
        // Could probably change that so processing dates were more grouped.
        // See: https://github.com/esbtools/event-handler/issues/11
        for (LockedResource<SharedIdentityEvents> identityLock : identityLocks.getLocks()) {
            try {
                identityLock.ensureAcquiredOrThrow("Won't update status or process event.");
            } catch (LostLockException e) {
                logger.warn("Lost lock. This is not fatal. See exception for details.", e);
                continue;
            }

            SharedIdentityEvents lockedEvents = identityLock.getResource();

            for (DocumentEventUpdate update : lockedEvents.updates.values()) {
                LightblueDocumentEvent event = update.event;
                DocumentEventEntity entity = event.wrappedDocumentEventEntity();

                // If id is null, this is a net new event as a result of merger.
                // See SharedIdentityEvents#addEvent(LightblueDocumentEvent)
                // and SharedIdentityEvents#update
                if (entity.get_id() == null) {
                    if (entity.getStatus().equals(DocumentEventEntity.Status.processing)) {
                        insertAndUpdateEvents.add(InsertRequests.documentEventsReturningOnlyIds(entity));
                        savedEvents.add(event);
                    }
                } else {
                    insertAndUpdateEvents.add(
                            UpdateRequests.documentEventStatusDatesAndSurvivorOfIfCurrent(
                                    entity, update.originalProcessingDate));
                    savedEvents.add(event);
                }
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

        Iterator<LightblueDocumentEvent> eventsIterator = savedEvents.iterator();
        Iterator<LightblueDataResponse> responsesIterator = bulkResponse.getResponses().iterator();

        while (eventsIterator.hasNext()) {
            if (!responsesIterator.hasNext()) {
                throw new IllegalStateException("Mismatched number of requests and responses! " +
                        "Events looked like: <{}>. Responses looked like");
            }

            LightblueDataResponse response = responsesIterator.next();
            LightblueDocumentEvent event = eventsIterator.next();
            DocumentEventEntity entity = event.wrappedDocumentEventEntity();

            if (LightblueErrors.arePresentInResponse(response)) {
                if (logger.isWarnEnabled()) {
                    List<String> errorStrings = LightblueErrors.toStringsFromErrorResponse(response);

                    logger.warn("Event update failed. Will not process. Event was: <{}>. " +
                            "Errors: <{}>", event, errorStrings);
                }
                eventsIterator.remove();
                continue;
            }

            if (response.parseModifiedCount() == 0) {
                logger.warn("Event updated by another thread. Will not process. " +
                        "Document event id: {}", event.wrappedDocumentEventEntity().get_id());
                eventsIterator.remove();
                continue;
            }

            // We don't care about returning events which are done processing. We only want events
            // which are supposed to be turned into publishable documents.
            if (!entity.getStatus().equals(DocumentEventEntity.Status.processing)) {
                eventsIterator.remove();
                continue;
            }

            // If known entity has no id, must've been insert. Populate id in returned entity.
            if (entity.get_id() == null) {
                DocumentEventEntity processed = response.parseProcessed(DocumentEventEntity.class);
                entity.set_id(processed.get_id());
            }
        }

        return savedEvents;
    }

    private static DocumentEventEntity asEntity(DocumentEvent event) {
        if (event instanceof LightblueDocumentEvent) {
            return ((LightblueDocumentEvent) event).wrappedDocumentEventEntity();
        }

        throw new IllegalArgumentException("Unknown event type. Only LightblueDocumentEvent is " +
                "supported. Event type was: " + event.getClass());
    }

    /**
     * Manages lightblue operations to be done as part of all events retrieved which belong to the
     * same lock (which is based on their {@link LightblueDocumentEvent#identity()}.
     */
    static class SharedIdentityEvents implements Lockable {
        final Identity identity;
        final Map<LightblueDocumentEvent, DocumentEventUpdate> updates = new IdentityHashMap<>();

        private final Optional<LockedResource<SharedIdentityEvents>> lock;
        // TODO: Is this guaranteed to only ever be one event?
        private final List<LightblueDocumentEvent> optimized = new ArrayList<>();
        private final Clock clock;

        /**
         * Attempts to parse {@code entities} into wrapping {@link LightblueDocumentEvent}
         * implementations provided by {@code documentEventFactoriesByType}, grouped by their
         * {@link Identity}.
         *
         * <p>As each entity is parsed, we attempt to lock its identity if we have not tried
         * already. If we are successful, we check if this event can be optimized among others with
         * the same identity (it almost certainly should be able to), and track the updates that
         * need to be persisted as a result of these optimizations.
         *
         * @param maxIdentities The maximum number of identities to lock, which <em>should</em>
         *                      also mean the maximum number of events, given all events with the
         *                      same identity should be able to be optimized down to one event.
         * @param entities The entities to parse.
         * @param requester The requester that parsed events will use to build documents.
         * @param documentEventFactoriesByType Tells us how to parse each entity into an event.
         * @param lockStrategy We only work on events we an lock. This is how we lock them.
         * @param clock Determines how we get timestamps. Mainly here for testing purposes.
         * @return All of the locked and optimized event batches, wrapped in a
         * {@link LockedResources} object which can be used to release the locks as well as to check
         * their current status.
         */
        static LockedResources<SharedIdentityEvents> parseAndOptimizeLockableDocumentEventEntities(
                int maxIdentities, DocumentEventEntity[] entities, LightblueRequester requester,
                Map<String, ? extends DocumentEventFactory> documentEventFactoriesByType,
                LockStrategy lockStrategy, Clock clock) {
            Map<Identity, SharedIdentityEvents> docEventsByIdentity = new HashMap<>();
            List<LockedResource<SharedIdentityEvents>> locksAcquired = new ArrayList<>();

            for (DocumentEventEntity eventEntity : entities) {
                String typeOfEvent = eventEntity.getCanonicalType();
                DocumentEventFactory eventFactoryForType = documentEventFactoriesByType.get(typeOfEvent);

                LightblueDocumentEvent newEvent;

                try {
                    newEvent = eventFactoryForType.getDocumentEventForEntity(eventEntity, requester);
                } catch (Exception e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Failed to parse event entity: " + eventEntity, e);
                    }

                    newEvent = new UnparseableDocumentEvent(e, eventEntity);
                }

                Identity identity = newEvent.identity();
                SharedIdentityEvents eventBatch = docEventsByIdentity.get(identity);

                if (eventBatch == null) {
                    if (locksAcquired.size() == maxIdentities) {
                        continue;
                    }

                    eventBatch = new SharedIdentityEvents(lockStrategy, identity, clock);
                    docEventsByIdentity.put(identity, eventBatch);
                    if (eventBatch.lock.isPresent()) {
                        locksAcquired.add(eventBatch.lock.get());
                        logger.debug("Acquired lock for resource {}", eventBatch.getResourceId());
                    }
                }

                eventBatch.addEvent(newEvent);
            }

            return LockedResources.fromLocks(locksAcquired);
        }

        /**
         * Attempts to acquire a lock on the provided {@code identity}. If the lock cannot be
         * acquired, the object is still created, but it will throw out any added events since we
         * cannot process them without a lock.
         */
        SharedIdentityEvents(LockStrategy lockStrategy, Identity identity, Clock clock) {
            this.identity = identity;
            this.clock = clock;

            Optional<LockedResource<SharedIdentityEvents>> lock;
            try {
                lock = Optional.of(lockStrategy.tryAcquire(this));
            } catch (LockNotAvailableException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Lock not available. This is not fatal. Assuming another" +
                            " thread is processing document events sharing identity: " + identity, e);
                }
                lock = Optional.empty();
            }

            this.lock = lock;
        }

        @Override
        public String getResourceId() {
            return identity.getResourceId();
        }

        @Override
        public String toString() {
            return "SharedIdentityEvents{" +
                    "identity=" + identity +
                    ", updates=" + updates +
                    ", optimized=" + optimized +
                    '}';
        }

        /**
         * Take the provided event and checks if it can be optimized among other known events of the
         * same identity. The results are tracked as side-effects to {@link #updates}.
         */
        private void addEvent(LightblueDocumentEvent event) {
            if (!Objects.equals(event.identity(), identity)) {
                throw new IllegalArgumentException("Tried to add event to shared identity batch " +
                        "that didn't share the same identity.");
            }

            if (!lock.isPresent()) {
                return;
            }

            // We have a new event, let's see if it is superseded by or can be merged with any
            // previous events we parsed or created as a result of a previous merge.

            // As we check, if we find we can merge an event, we will merge it, and continue on with
            // the merger instead. These pointers track which event we are currently optimizing.
            @Nullable LightblueDocumentEvent newOrMergerEvent = event;
            DocumentEventEntity newOrMergerEventEntity = event.wrappedDocumentEventEntity();

            Iterator<LightblueDocumentEvent> optimizedIterator = optimized.iterator();

            while (optimizedIterator.hasNext()) {
                LightblueDocumentEvent previouslyOptimizedEvent = optimizedIterator.next();

                if (newOrMergerEvent.isSupersededBy(previouslyOptimizedEvent)) {
                    // Keep previous event...
                    DocumentEventEntity previousEntity = previouslyOptimizedEvent.wrappedDocumentEventEntity();
                    previousEntity.addSurvivorOfIds(newOrMergerEventEntity.getSurvivorOfIds());
                    previousEntity.addSurvivorOfIds(newOrMergerEventEntity.get_id());

                    // ...and throw away this new one (which means simply drop it if it is a net new
                    // event as a result of a merge which will have no id yet).
                    if (newOrMergerEventEntity.get_id() != null) {
                        newOrMergerEventEntity.setStatus(DocumentEventEntity.Status.superseded);
                        updates.put(newOrMergerEvent, DocumentEventUpdate.timestamp(newOrMergerEvent, clock));
                    }

                    if (logger.isDebugEnabled()) {
                        logger.debug("Event {} superseded by event {}",
                                identify(newOrMergerEventEntity), identify(previousEntity));
                    }

                    newOrMergerEventEntity.setSurvivorOfIds(null);
                    newOrMergerEvent = null;

                    break;
                } else if (previouslyOptimizedEvent.isSupersededBy(newOrMergerEvent)) {
                    // Previous entity was processing; now it is superseded and removed from
                    // optimized result list.
                    DocumentEventEntity previousEntity = previouslyOptimizedEvent.wrappedDocumentEventEntity();
                    if (previousEntity.get_id() == null) {
                        // Was net-new event from merger, but we aren't going to process, so ignore.
                        updates.remove(previouslyOptimizedEvent);
                    } else {
                        previousEntity.setStatus(DocumentEventEntity.Status.superseded);
                        previousEntity.setProcessedDate(ZonedDateTime.now(clock));
                    }
                    optimizedIterator.remove();

                    newOrMergerEventEntity.addSurvivorOfIds(previousEntity.get_id());
                    newOrMergerEventEntity.addSurvivorOfIds(previousEntity.getSurvivorOfIds());

                    if (logger.isDebugEnabled()) {
                        logger.debug("Event {} superseded by event {}",
                                identify(previousEntity), identify(newOrMergerEventEntity));
                    }

                    previousEntity.setSurvivorOfIds(null);
                } else if (newOrMergerEvent.couldMergeWith(previouslyOptimizedEvent)) {
                    // Previous entity was processing; now it is merged and removed from optimized
                    // result list.
                    DocumentEventEntity previousEntity = previouslyOptimizedEvent.wrappedDocumentEventEntity();
                    if (previousEntity.get_id() == null) {
                        // Was net-new event from merger, but we aren't going to process, so ignore.
                        updates.remove(previouslyOptimizedEvent);
                    } else {
                        previousEntity.setStatus(DocumentEventEntity.Status.merged);
                        previousEntity.setProcessedDate(ZonedDateTime.now(clock));
                    }
                    optimizedIterator.remove();

                    // This new event will not be included in result list, but we do have to update
                    // its entity (if it has one) to store that it has been merged.
                    newOrMergerEventEntity.setStatus(DocumentEventEntity.Status.merged);
                    if (newOrMergerEventEntity.get_id() != null) {
                        updates.put(newOrMergerEvent, DocumentEventUpdate.timestamp(newOrMergerEvent, clock));
                    }

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

                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "Events {} and {} merged into new event which now merges all of {}",
                                identify(previousEntity), identify(newOrMergerEventEntity),
                                identify(mergerEntity));
                    }

                    newOrMergerEventEntity.setSurvivorOfIds(null);
                    previousEntity.setSurvivorOfIds(null);

                    newOrMergerEvent = merger;
                    newOrMergerEventEntity = mergerEntity;
                }
            }

            if (newOrMergerEvent != null) {
                newOrMergerEventEntity.setStatus(DocumentEventEntity.Status.processing);
                optimized.add(newOrMergerEvent);
                updates.put(newOrMergerEvent, DocumentEventUpdate.timestamp(newOrMergerEvent, clock));
            }
        }
    }

    /**
     * @return A concise string identifying the event in some way whether it has an existing id or
     * not.
     */
    static String identify(DocumentEventEntity eventEntity) {
        return eventEntity.get_id() != null
                ? "<id=" + eventEntity.get_id() + ">"
                : "<survivorOfIds=" + eventEntity.getSurvivorOfIds() + ">";
    }

    /**
     * Captures a new or changed event with its to-be-persisted state, and the original processing
     * timestamp of the currently persisted state (or null if we're processing the event for the
     * first time.)
     */
    static class DocumentEventUpdate {
        final @Nullable ZonedDateTime originalProcessingDate;
        final LightblueDocumentEvent event;

        /**
         * Updates the {@link DocumentEventEntity#setProcessingDate(ZonedDateTime) processing date}
         * and potentially also the {@link DocumentEventEntity#setProcessedDate(ZonedDateTime)
         * processed date} of the provided event's entity, keeping track of the original processing
         * date timestamp to catch concurrent modifications to the same entity.
         *
         * <p><strong>You must not update the event's entity's processing date yourself. This will
         * do that for you.</strong> Similarly, an event must not be timestamped more than once.
         *
         * <p>The processed date is only updated if the status of the entity is
         * {@link DocumentEventEntity.Status#superseded},
         * {@link DocumentEventEntity.Status#merged}, or
         * {@link DocumentEventEntity.Status#published}.
         *
         * <p>It is fine to mutate the event's entity further after it has been timestamped. You do
         * not need to timestamp it again. In fact, you absolutely should not do that.
         */
        static DocumentEventUpdate timestamp(LightblueDocumentEvent event, Clock clock) {
            DocumentEventEntity entity = event.wrappedDocumentEventEntity();
            DocumentEventEntity.Status currentStatus = entity.getStatus();

            ZonedDateTime originalProcessingDate = entity.getProcessingDate();
            ZonedDateTime now = ZonedDateTime.now(clock);

            entity.setProcessingDate(now);

            if (DocumentEventEntity.Status.superseded.equals(currentStatus) ||
                    DocumentEventEntity.Status.merged.equals(currentStatus) ||
                    // TODO: Is published check needed?
                    DocumentEventEntity.Status.published.equals(currentStatus)) {
                entity.setProcessedDate(now);
            }

            return new DocumentEventUpdate(originalProcessingDate, event);
        }

        private DocumentEventUpdate(
                @Nullable ZonedDateTime originalProcessingDate,
                LightblueDocumentEvent event) {
            this.originalProcessingDate = originalProcessingDate;
            this.event = event;
        }
    }
}
