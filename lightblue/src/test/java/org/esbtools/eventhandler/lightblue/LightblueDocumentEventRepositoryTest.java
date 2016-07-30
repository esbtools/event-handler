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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;

import org.esbtools.eventhandler.FailedDocumentEvent;
import org.esbtools.eventhandler.lightblue.config.MutableLightblueDocumentEventRepositoryConfig;
import org.esbtools.eventhandler.lightblue.testing.InMemoryLockStrategy;
import org.esbtools.eventhandler.lightblue.testing.LightblueClientConfigurations;
import org.esbtools.eventhandler.lightblue.testing.LightblueClients;
import org.esbtools.eventhandler.lightblue.testing.MultiStringDocumentEvent;
import org.esbtools.eventhandler.lightblue.testing.SlowDataLightblueClient;
import org.esbtools.eventhandler.lightblue.testing.StringDocumentEvent;
import org.esbtools.eventhandler.lightblue.testing.TestLogger;
import org.esbtools.eventhandler.lightblue.testing.TestMetadataJson;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueClientConfiguration;
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.integration.test.LightblueExternalResource;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.DataInsertRequest;
import com.redhat.lightblue.client.request.data.DataSaveRequest;

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class LightblueDocumentEventRepositoryTest {
    @ClassRule
    public static LightblueExternalResource lightblueExternalResource =
            new LightblueExternalResource(TestMetadataJson.forEntity(DocumentEventEntity.class));

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TestLogger testLogger = new TestLogger();

    private LightblueClient client;

    private LightblueDocumentEventRepository repository;

    private Clock fixedClock = Clock.fixed(Instant.now(), ZoneId.of("GMT"));

    private static final int DOCUMENT_EVENT_BATCH_SIZE = 10;
    private static final Duration PROCESSING_TIMEOUT = Duration.ofMinutes(1);
    private static final Duration EXPIRE_THRESHOLD = Duration.ofSeconds(30);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(15);

    private Map<String, DocumentEventFactory> documentEventFactoriesByType =
            new HashMap<String, DocumentEventFactory>() {{
                put("String", StringDocumentEvent::new);
                put("MultiString", MultiStringDocumentEvent::new);
                put("Fails", (entity, requester) -> { throw new RuntimeException("Uh oh"); });
            }};

    private MutableLightblueDocumentEventRepositoryConfig config =
            new MutableLightblueDocumentEventRepositoryConfig(documentEventFactoriesByType.keySet(),
                    DOCUMENT_EVENT_BATCH_SIZE, PROCESSING_TIMEOUT, EXPIRE_THRESHOLD);

    private InMemoryLockStrategy lockStrategy = new InMemoryLockStrategy();

    @Before
    public void initializeLightblueClientAndRepository() {
        LightblueClientConfiguration lbClientConfig = LightblueClientConfigurations
                .fromLightblueExternalResource(lightblueExternalResource);
        client = LightblueClients.withJavaTimeSerializationSupport(lbClientConfig);

        // TODO: Try and reduce places canonical types are specified
        // We have 3 here: type list to process, types to factories, and inside the doc event impls
        // themselves.
        repository = new LightblueDocumentEventRepository(client, lockStrategy, config,
                documentEventFactoriesByType, fixedClock);
    }

    @Before
    public void dropDocEventEntities() throws UnknownHostException {
        lightblueExternalResource.cleanupMongoCollections(DocumentEventEntity.ENTITY_NAME);
    }

    @Test
    public void shouldRetrieveDocumentEventsForSpecifiedEntities() throws Exception {
        DocumentEventEntity stringEvent = newStringDocumentEventEntity("foo");

        DocumentEventEntity otherEvent = DocumentEventEntity.newlyCreated(null, "Other", 50,
                ZonedDateTime.now(fixedClock), new DocumentEventEntity.KeyAndValue("value", "foo"));

        insertDocumentEventEntities(stringEvent, otherEvent);

        List<LightblueDocumentEvent> docEvents = repository.retrievePriorityDocumentEventsUpTo(2);

        assertEquals(1, docEvents.size());

        DocumentEventEntity entity = docEvents.get(0).wrappedDocumentEventEntity();

        assertEquals(stringEvent.getCanonicalType(), entity.getCanonicalType());
        assertEquals(stringEvent.getParameters(), entity.getParameters());
        assertEquals(stringEvent.getCreationDate().toInstant(), entity.getCreationDate().toInstant());
    }

    @Test
    public void shouldMarkRetrievedDocumentEventsAsProcessing() throws Exception {
        DocumentEventEntity stringEvent1 = newStringDocumentEventEntity("foo");
        DocumentEventEntity stringEvent2 = newStringDocumentEventEntity("bar");

        insertDocumentEventEntities(stringEvent1, stringEvent2);

        repository.retrievePriorityDocumentEventsUpTo(2);

        DataFindRequest findDocEvent = new DataFindRequest(DocumentEventEntity.ENTITY_NAME, DocumentEventEntity.VERSION);
        findDocEvent.select(Projection.includeFieldRecursively("*"));
        findDocEvent.where(Query.withValue("canonicalType", Query.BinOp.eq, "String"));
        DocumentEventEntity[] found = client.data(findDocEvent).parseProcessed(DocumentEventEntity[].class);

        assertEquals(DocumentEventEntity.Status.processing, found[0].getStatus());
        assertEquals(DocumentEventEntity.Status.processing, found[1].getStatus());
    }

    @Test
    public void shouldOnlyRetrieveUnprocessedEvents() throws Exception {
        DocumentEventEntity stringEvent = newStringDocumentEventEntity("right");
        DocumentEventEntity processingEvent = newStringDocumentEventEntity("wrong");
        processingEvent.setStatus(DocumentEventEntity.Status.processing);
        DocumentEventEntity processedEvent = newStringDocumentEventEntity("wrong");
        processedEvent.setStatus(DocumentEventEntity.Status.published);
        DocumentEventEntity failedEvent = newStringDocumentEventEntity("wrong");
        failedEvent.setStatus(DocumentEventEntity.Status.failed);
        DocumentEventEntity mergedEvent = newStringDocumentEventEntity("wrong");
        mergedEvent.setStatus(DocumentEventEntity.Status.merged);
        DocumentEventEntity supersededEvent = newStringDocumentEventEntity("wrong");
        supersededEvent.setStatus(DocumentEventEntity.Status.superseded);

        insertDocumentEventEntities(stringEvent, processingEvent, processedEvent, failedEvent,
                mergedEvent, supersededEvent);

        List<LightblueDocumentEvent> retrieved = repository.retrievePriorityDocumentEventsUpTo(6);

        assertEquals(1, retrieved.size());
        assertEquals("right", retrieved.get(0).wrappedDocumentEventEntity().getParameterByKey("value"));
    }

    @Test
    @Ignore("Flakey. For some reason repositories are not always retrieving all 20 events in " +
            "their event batch.")
    public void shouldReturnNonOverlappingSetsEvenAmongMultipleThreads() throws LightblueException,
            InterruptedException, ExecutionException, TimeoutException {
        SlowDataLightblueClient thread1Client = new SlowDataLightblueClient(client);
        SlowDataLightblueClient thread2Client = new SlowDataLightblueClient(client);

        // Larger batch size (20) is needed to guarantee we get all events in both threads.
        MutableLightblueDocumentEventRepositoryConfig config =
                new MutableLightblueDocumentEventRepositoryConfig(
                        documentEventFactoriesByType.keySet(), 20, Duration.ofMinutes(10),
                        Duration.ofMinutes(1));

        LightblueDocumentEventRepository thread1Repository = new LightblueDocumentEventRepository(
                thread1Client, new InMemoryLockStrategy(), config,
                documentEventFactoriesByType, fixedClock);
        LightblueDocumentEventRepository thread2Repository = new LightblueDocumentEventRepository(
                thread2Client, new InMemoryLockStrategy(), config,
                documentEventFactoriesByType, fixedClock);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            DocumentEventEntity[] entities = randomNewDocumentEventEntities(20);

            List<String> expectedValues = Arrays.stream(entities)
                    .map(e -> e.getParameterByKey("value"))
                    .collect(Collectors.toList());

            insertDocumentEventEntities(entities);

            CountDownLatch bothThreadsStarted = new CountDownLatch(2);

            thread1Client.pauseBeforeRequests();
            thread2Client.pauseBeforeRequests();

            Future<List<LightblueDocumentEvent>> futureThread1Events = executor.submit(() -> {
                bothThreadsStarted.countDown();
                return thread1Repository.retrievePriorityDocumentEventsUpTo(15);
            });
            Future<List<LightblueDocumentEvent>> futureThread2Events = executor.submit(() -> {
                bothThreadsStarted.countDown();
                return thread2Repository.retrievePriorityDocumentEventsUpTo(15);
            });

            bothThreadsStarted.await();

            // This sleep is a bit hacky but, testing concurrency is hard...
            Thread.sleep(5000);

            thread2Client.unpause();
            thread1Client.unpause();

            List<LightblueDocumentEvent> thread1Events = futureThread1Events.get(10, TimeUnit.SECONDS);
            List<LightblueDocumentEvent> thread2Events = futureThread2Events.get(10, TimeUnit.SECONDS);

            List<String> retrievedValues = new ArrayList<>();

            retrievedValues.addAll(thread1Events.stream()
                    .map(e -> e.wrappedDocumentEventEntity().getParameterByKey("value"))
                    .collect(Collectors.toList()));

            retrievedValues.addAll(thread2Events.stream()
                    .map(e -> e.wrappedDocumentEventEntity().getParameterByKey("value"))
                    .collect(Collectors.toList()));

            assertThat(retrievedValues).containsExactlyElementsIn(expectedValues);
        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    @Test
    public void shouldRetrieveTopPriorityDocumentEventsInBatch() throws Exception {
        insertDocumentEventEntities(
                newRandomStringDocumentEventEntityWithPriorityOverride(5),
                newRandomStringDocumentEventEntityWithPriorityOverride(100),
                newRandomStringDocumentEventEntityWithPriorityOverride(50),
                newRandomStringDocumentEventEntityWithPriorityOverride(25),
                newRandomStringDocumentEventEntityWithPriorityOverride(30),
                newRandomStringDocumentEventEntityWithPriorityOverride(25),
                newRandomStringDocumentEventEntityWithPriorityOverride(99),
                newRandomStringDocumentEventEntityWithPriorityOverride(55),
                newRandomStringDocumentEventEntityWithPriorityOverride(70),
                newRandomStringDocumentEventEntityWithPriorityOverride(2));

        List<Integer> priorities = repository.retrievePriorityDocumentEventsUpTo(5).stream()
                .map(LightblueDocumentEvent::wrappedDocumentEventEntity)
                .map(DocumentEventEntity::getPriority)
                .collect(Collectors.toList());

        assertThat(priorities).containsExactly(100, 99, 70, 55, 50);
    }

    @Test
    public void shouldIgnoreSupersededEventsAndMarkAsSupersededAndTrackVictimIds() throws Exception {
        Clock creationTimeClock = Clock.offset(fixedClock, Duration.ofHours(1).negated());
        Instant expectedProcessedDate = ZonedDateTime.now(fixedClock).toInstant();

        insertDocumentEventEntities(
                newStringDocumentEventEntity("duplicate", creationTimeClock),
                newStringDocumentEventEntity("duplicate", creationTimeClock),
                newStringDocumentEventEntity("duplicate", creationTimeClock),
                newStringDocumentEventEntity("duplicate", creationTimeClock),
                newStringDocumentEventEntity("duplicate", creationTimeClock));

        List<LightblueDocumentEvent> retrieved = repository.retrievePriorityDocumentEventsUpTo(5);

        List<DocumentEventEntity> supersededEntities = findDocumentEventEntitiesWhere(
                Query.withValue("status", Query.BinOp.eq, DocumentEventEntity.Status.superseded));
        List<DocumentEventEntity> survivorEntities = findDocumentEventEntitiesWhere(
                Query.withValue("status", Query.BinOp.neq, DocumentEventEntity.Status.superseded));

        assertEquals(1, retrieved.size());
        assertEquals(1, survivorEntities.size());
        assertEquals(
                Arrays.asList(expectedProcessedDate, expectedProcessedDate, expectedProcessedDate, expectedProcessedDate),
                supersededEntities.stream()
                        .map(DocumentEventEntity::getProcessedDate)
                        .map(ZonedDateTime::toInstant)
                        .collect(Collectors.toList()));

        DocumentEventEntity retrievedEntity = survivorEntities.get(0);
        DocumentEventEntity survivorEntity = survivorEntities.get(0);

        List<String> supersededIds = supersededEntities.stream()
                .map(DocumentEventEntity::get_id)
                .collect(Collectors.toList());
        List<String> supersededSurvivorOfIds = supersededEntities.stream()
                .flatMap(e -> Optional.ofNullable(e.getSurvivorOfIds())
                        .orElse(Collections.emptySet())
                        .stream())
                .collect(Collectors.toList());

        assertThat(survivorEntity.getSurvivorOfIds()).containsExactlyElementsIn(supersededIds);
        assertThat(retrievedEntity.getSurvivorOfIds()).containsExactlyElementsIn(supersededIds);
        assertThat(supersededSurvivorOfIds).named("superseded entities survivor of ids").isEmpty();
    }

    @Test
    public void shouldCheckIfLowerPriorityEventsSupersedeHigherPriorityEventsAndMarkAsSupersededAndTrackVictimsIds()
            throws Exception {
        Instant expectedProcessedDate = ZonedDateTime.now(fixedClock).toInstant();

        insertDocumentEventEntitiesInOrder(
                newMultiStringDocumentEventEntity("1"),
                newMultiStringDocumentEventEntity("1", "2"),
                newMultiStringDocumentEventEntity("1", "2", "3"),
                newMultiStringDocumentEventEntity("1", "2", "3", "4"),
                newMultiStringDocumentEventEntity("1", "2", "3", "4", "5"));

        List<LightblueDocumentEvent> retrieved = repository.retrievePriorityDocumentEventsUpTo(5);

        List<DocumentEventEntity> supersededEntities = findDocumentEventEntitiesWhere(
                Query.withValue("status", Query.BinOp.eq, DocumentEventEntity.Status.superseded));
        List<DocumentEventEntity> survivorEntities = findDocumentEventEntitiesWhere(
                Query.withValue("status", Query.BinOp.neq, DocumentEventEntity.Status.superseded));

        assertEquals(1, retrieved.size());
        assertEquals(1, survivorEntities.size());
        assertEquals(
                Arrays.asList(expectedProcessedDate, expectedProcessedDate, expectedProcessedDate, expectedProcessedDate),
                supersededEntities.stream()
                        .map(DocumentEventEntity::getProcessedDate)
                        .map(ZonedDateTime::toInstant)
                        .collect(Collectors.toList()));

        MultiStringDocumentEvent event = (MultiStringDocumentEvent) retrieved.get(0);
        DocumentEventEntity retrievedEntity = event.wrappedDocumentEventEntity();
        DocumentEventEntity survivorEntity = survivorEntities.get(0);

        List<String> supersededIds = supersededEntities.stream()
                .map(DocumentEventEntity::get_id)
                .collect(Collectors.toList());

        List<String> supersededSurvivorOfIds = supersededEntities.stream()
                .flatMap(e -> Optional.ofNullable(e.getSurvivorOfIds())
                        .orElse(Collections.emptySet())
                        .stream())
                .collect(Collectors.toList());

        assertThat(retrievedEntity.getSurvivorOfIds()).containsExactlyElementsIn(supersededIds);
        assertThat(survivorEntity.getSurvivorOfIds()).containsExactlyElementsIn(supersededIds);
        assertThat(event.values()).containsExactly("1", "2", "3", "4", "5");
        assertThat(supersededSurvivorOfIds).named("superseded entities survivor of ids").isEmpty();
    }

    @Test
    public void shouldMergeEventsInBatchAndMarkAsMergedAndTrackVictims() throws Exception {
        insertDocumentEventEntities(
                newMultiStringDocumentEventEntity("1"),
                newMultiStringDocumentEventEntity("2"),
                newMultiStringDocumentEventEntity("3"),
                newMultiStringDocumentEventEntity("4"),
                newMultiStringDocumentEventEntity("5"));

        List<LightblueDocumentEvent> retrieved = repository.retrievePriorityDocumentEventsUpTo(5);

        List<DocumentEventEntity> mergedEntities = findDocumentEventEntitiesWhere(
                Query.withValue("status", Query.BinOp.eq, DocumentEventEntity.Status.merged));
        List<DocumentEventEntity> nonMergedEntities = findDocumentEventEntitiesWhere(
                Query.withValue("status", Query.BinOp.neq, DocumentEventEntity.Status.merged));

        assertThat(retrieved).hasSize(1);
        assertThat(nonMergedEntities).hasSize(1);
        assertThat(mergedEntities).named("merged entities").hasSize(5);

        DocumentEventEntity retrievedEntity = retrieved.get(0).wrappedDocumentEventEntity();
        DocumentEventEntity processingEntity = nonMergedEntities.get(0);

        List<String> mergedIds = mergedEntities.stream()
                .map(DocumentEventEntity::get_id)
                .collect(Collectors.toList());
        List<String> mergedSurvivorOfIds = mergedEntities.stream()
                .flatMap(e -> Optional.ofNullable(e.getSurvivorOfIds())
                        .orElse(Collections.emptySet())
                        .stream())
                .collect(Collectors.toList());

        assertThat(retrievedEntity.getSurvivorOfIds()).containsExactlyElementsIn(mergedIds);
        assertThat(processingEntity.getSurvivorOfIds()).containsExactlyElementsIn(mergedIds);
        assertThat(mergedSurvivorOfIds).named("merged entities survivor of ids").isEmpty();
    }

    @Test
    public void shouldPersistSurvivorEntitiesAsProcessingAndUpdateRetrievedWithIdIfCreatedDuringRetrieval()
            throws Exception {
        insertDocumentEventEntities(
                newMultiStringDocumentEventEntity("1"),
                newMultiStringDocumentEventEntity("2"),
                newMultiStringDocumentEventEntity("3"),
                newMultiStringDocumentEventEntity("4"),
                newMultiStringDocumentEventEntity("5"));

        List<LightblueDocumentEvent> retrieved = repository.retrievePriorityDocumentEventsUpTo(5);

        List<DocumentEventEntity> processingEntities = findDocumentEventEntitiesWhere(
                Query.withValue("status", Query.BinOp.eq, DocumentEventEntity.Status.processing));

        assertThat(retrieved).hasSize(1);

        DocumentEventEntity retrievedEntity = retrieved.get(0).wrappedDocumentEventEntity();
        DocumentEventEntity processingEntity = processingEntities.get(0);

        assertThat(processingEntities).named("persisted processing entities").hasSize(1);
        assertThat(retrievedEntity.get_id()).named("retrieved event id")
                .isEqualTo(processingEntity.get_id());
    }

    @Test
    public void shouldReturnNoMoreThanMaxEventsDespiteBatchSizeAndLeaveRemainingEventsUnprocessed()
            throws Exception {
        insertDocumentEventEntities(randomNewDocumentEventEntities(5));

        List<LightblueDocumentEvent> returnedEvents = repository.retrievePriorityDocumentEventsUpTo(2);

        List<DocumentEventEntity> unprocessedEntities = findDocumentEventEntitiesWhere(
                Query.withValue("status", Query.BinOp.eq, DocumentEventEntity.Status.unprocessed));

        assertThat(returnedEvents).hasSize(2);
        assertThat(unprocessedEntities).hasSize(3);
        assertThat(returnedEvents.stream()
                .map(LightblueDocumentEvent::wrappedDocumentEventEntity)
                .map(DocumentEventEntity::get_id)
                .collect(Collectors.toList()))
                .containsNoneIn(unprocessedEntities.stream()
                        .map(DocumentEventEntity::get_id)
                        .collect(Collectors.toList()));
    }

    @Test
    public void shouldSearchThroughNoMoreThanBatchSize() throws Exception {
        insertDocumentEventEntities(randomNewDocumentEventEntities(DOCUMENT_EVENT_BATCH_SIZE + 1));

        List<LightblueDocumentEvent> returnedEvents =
                repository.retrievePriorityDocumentEventsUpTo(DOCUMENT_EVENT_BATCH_SIZE + 1);

        List<DocumentEventEntity> unprocessedEntities = findDocumentEventEntitiesWhere(
                Query.withValue("status", Query.BinOp.eq, DocumentEventEntity.Status.unprocessed));

        assertThat(returnedEvents).hasSize(DOCUMENT_EVENT_BATCH_SIZE);
        assertThat(unprocessedEntities).hasSize(1);
        assertThat(returnedEvents.stream()
                .map(LightblueDocumentEvent::wrappedDocumentEventEntity)
                .map(DocumentEventEntity::get_id)
                .collect(Collectors.toList()))
                .containsNoneIn(unprocessedEntities.stream()
                        .map(DocumentEventEntity::get_id)
                        .collect(Collectors.toList()));
    }

    @Test
    public void shouldOnlyReturnUpToMaxEventsAskedForButShouldOptimizeAmongUpToBatchSizeEvents()
            throws Exception {
        // 4 which are not able to be merged, and 7 which can be.
        // Batch size should be 10, so one of these will remain unprocessed.
        insertDocumentEventEntitiesInOrder(
                newMultiStringDocumentEventEntity("should merge 1"),
                newStringDocumentEventEntity("1"),
                newMultiStringDocumentEventEntity("should merge 2"),
                newStringDocumentEventEntity("2"),
                newMultiStringDocumentEventEntity("should merge 3"),
                newStringDocumentEventEntity("3"),
                newMultiStringDocumentEventEntity("should merge 4"),
                newMultiStringDocumentEventEntity("should merge 5"),
                newStringDocumentEventEntity("4"),
                newMultiStringDocumentEventEntity("should merge 6"),
                newMultiStringDocumentEventEntity("should merge 7"));

        List<LightblueDocumentEvent> returnedEvents = repository.retrievePriorityDocumentEventsUpTo(5);

        List<StringDocumentEvent> returnedStringEvents = returnedEvents.stream()
                .filter(e -> e instanceof StringDocumentEvent)
                .map(e -> (StringDocumentEvent) e)
                .collect(Collectors.toList());

        List<MultiStringDocumentEvent> returnedMultiStringEvents = returnedEvents.stream()
                .filter(e -> e instanceof MultiStringDocumentEvent)
                .map(e -> (MultiStringDocumentEvent) e)
                .collect(Collectors.toList());

        assertThat(returnedEvents).hasSize(5);
        assertThat(returnedStringEvents.stream()
                .map(StringDocumentEvent::value)
                .collect(Collectors.toList()))
                .containsExactly("1", "2", "3", "4");
        assertThat(returnedMultiStringEvents).hasSize(1);
        assertThat(returnedMultiStringEvents.get(0).values()).containsExactly(
                "should merge 1", "should merge 2", "should merge 3", "should merge 4",
                "should merge 5", "should merge 6");
    }

    @Test
    public void shouldAddNewDocumentEventsEntitiesAsUnprocessedAndShouldNotUpdateExistingEventsWithIds()
            throws LightblueException {
        StringDocumentEvent event1 = new StringDocumentEvent(null, "1", fixedClock);
        MultiStringDocumentEvent event2 = new MultiStringDocumentEvent(null, Arrays.asList("2"), fixedClock);

        List<LightblueDocumentEvent> events = new ArrayList<>();
        events.add(event1);
        events.add(event2);

        repository.addNewDocumentEvents(events);

        List<DocumentEventEntity> stringEventEntities = findDocumentEventEntitiesWhere(
                Query.withValue("canonicalType", Query.BinOp.eq, "String"));
        List<DocumentEventEntity> multiStringEventEntities = findDocumentEventEntitiesWhere(
                Query.withValue("canonicalType", Query.BinOp.eq, "MultiString"));

        assertThat(stringEventEntities).hasSize(1);
        assertThat(multiStringEventEntities).hasSize(1);

        DocumentEventEntity entity1 = stringEventEntities.get(0);
        DocumentEventEntity entity2 = multiStringEventEntities.get(0);
        // Original entities shouldn't get ids.
        // The only reason for this is current complexity that that would require, in addition to
        // it not being strictly necessary. That is, if it were easy to do, it would probably be a
        // good idea to update the entities with the persisted ids actually. But it's not that easy
        // right now, so not worrying about it.
        // See: https://github.com/lightblue-platform/lightblue-core/issues/556
        entity1.set_id(null);
        entity2.set_id(null);

        assertEquals(event1.wrappedDocumentEventEntity(), entity1);
        assertEquals(event2.wrappedDocumentEventEntity(), entity2);
    }

    @Test
    public void shouldAddNewDocumentEventsInMultipleRequestsLimitedByMaximumEventsPerInsertIfSet()
            throws Exception {
        MutableLightblueDocumentEventRepositoryConfig max5EventsPerInsert =
                new MutableLightblueDocumentEventRepositoryConfig(Collections.emptyList(),
                        0, Optional.of(5), Duration.ZERO, Duration.ZERO);

        SlowDataLightblueClient slowClient = new SlowDataLightblueClient(client);

        Collection<StringDocumentEvent> newEvents = randomNewStringDocumentEvents(13);

        repository = new LightblueDocumentEventRepository(slowClient, lockStrategy,
                max5EventsPerInsert, documentEventFactoriesByType, fixedClock);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        slowClient.pauseBeforeRequests();

        try {
            Future<?> addEventsFuture = executor.submit(() -> {
                repository.addNewDocumentEvents(newEvents);
                return null;
            });

            slowClient.waitUntilPausedRequestQueuedAtMost(REQUEST_TIMEOUT);
            slowClient.flushPendingRequest();
            slowClient.waitUntilPausedRequestQueuedAtMost(REQUEST_TIMEOUT);

            assertThat(findDocumentEventEntitiesWhere(null)).hasSize(5);

            slowClient.flushPendingRequest();
            slowClient.waitUntilPausedRequestQueuedAtMost(REQUEST_TIMEOUT);

            assertThat(findDocumentEventEntitiesWhere(null)).hasSize(10);

            slowClient.flushPendingRequest();
            addEventsFuture.get(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            List<DocumentEventEntity> allInserted = findDocumentEventEntitiesWhere(null);

            assertThat(allInserted).hasSize(13);

            List<String> insertedValues = allInserted.stream()
                    .map(StringDocumentEvent::new)
                    .map(StringDocumentEvent::value)
                    .collect(Collectors.toList());

            List<String> expectedValues = newEvents.stream()
                    .map(StringDocumentEvent::value)
                    .collect(Collectors.toList());

            assertThat(insertedValues).containsExactlyElementsIn(expectedValues);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void shouldAddNewDocumentEventsInOneRequestIfMaxEventsPerInsertSetButEventCountIsLessThanMax()
            throws Exception {
        MutableLightblueDocumentEventRepositoryConfig max5EventsPerInsert =
                new MutableLightblueDocumentEventRepositoryConfig(Collections.emptyList(),
                        0, Optional.of(5), Duration.ZERO, Duration.ZERO);

        SlowDataLightblueClient slowClient = new SlowDataLightblueClient(client);

        Collection<StringDocumentEvent> newEvents = randomNewStringDocumentEvents(3);

        repository = new LightblueDocumentEventRepository(slowClient, lockStrategy,
                max5EventsPerInsert, documentEventFactoriesByType, fixedClock);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        slowClient.pauseBeforeRequests();

        try {
            Future<?> addEventsFuture = executor.submit(() -> {
                repository.addNewDocumentEvents(newEvents);
                return null;
            });

            slowClient.waitUntilPausedRequestQueuedAtMost(REQUEST_TIMEOUT);
            slowClient.flushPendingRequest();

            addEventsFuture.get(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            List<DocumentEventEntity> allInserted = findDocumentEventEntitiesWhere(null);

            assertThat(allInserted).hasSize(3);

            List<String> insertedValues = allInserted.stream()
                    .map(StringDocumentEvent::new)
                    .map(StringDocumentEvent::value)
                    .collect(Collectors.toList());

            List<String> expectedValues = newEvents.stream()
                    .map(StringDocumentEvent::value)
                    .collect(Collectors.toList());

            assertThat(insertedValues).containsExactlyElementsIn(expectedValues);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void shouldAddNewDocumentEventsInOneRequestIfMaxEventsPerInsertSetButEventCountIsEqualToMax()
            throws Exception {
        MutableLightblueDocumentEventRepositoryConfig max5EventsPerInsert =
                new MutableLightblueDocumentEventRepositoryConfig(Collections.emptyList(),
                        0, Optional.of(5), Duration.ZERO, Duration.ZERO);

        SlowDataLightblueClient slowClient = new SlowDataLightblueClient(client);

        Collection<StringDocumentEvent> newEvents = randomNewStringDocumentEvents(5);

        repository = new LightblueDocumentEventRepository(slowClient, lockStrategy,
                max5EventsPerInsert, documentEventFactoriesByType, fixedClock);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        slowClient.pauseBeforeRequests();

        try {
            Future<?> addEventsFuture = executor.submit(() -> {
                repository.addNewDocumentEvents(newEvents);
                return null;
            });

            slowClient.waitUntilPausedRequestQueuedAtMost(REQUEST_TIMEOUT);
            slowClient.flushPendingRequest();

            addEventsFuture.get(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            List<DocumentEventEntity> allInserted = findDocumentEventEntitiesWhere(null);

            assertThat(allInserted).hasSize(5);

            List<String> insertedValues = allInserted.stream()
                    .map(StringDocumentEvent::new)
                    .map(StringDocumentEvent::value)
                    .collect(Collectors.toList());

            List<String> expectedValues = newEvents.stream()
                    .map(StringDocumentEvent::value)
                    .collect(Collectors.toList());

            assertThat(insertedValues).containsExactlyElementsIn(expectedValues);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void shouldUpdateProcessedEntitiesWithStatusAndDatePostPublishing() throws LightblueException {
        Clock creationTimeClock = Clock.offset(fixedClock, Duration.ofHours(1).negated());

        StringDocumentEvent event1 = new StringDocumentEvent(null, "1", creationTimeClock);
        StringDocumentEvent event2 = new StringDocumentEvent(null, "2", creationTimeClock);
        DocumentEventEntity entity1 = event1.wrappedDocumentEventEntity();
        DocumentEventEntity entity2 = event2.wrappedDocumentEventEntity();
        entity1.set_id("1");
        entity2.set_id("2");
        entity1.setStatus(DocumentEventEntity.Status.processing);
        entity2.setStatus(DocumentEventEntity.Status.processing);

        DataInsertRequest insertProcessingEntities = new DataInsertRequest(
                DocumentEventEntity.ENTITY_NAME, DocumentEventEntity.VERSION);
        insertProcessingEntities.create(entity1, entity2);
        client.data(insertProcessingEntities);

        List<LightblueDocumentEvent> successes = Arrays.asList(event1);
        List<FailedDocumentEvent> failures = Arrays.asList(new FailedDocumentEvent(event2, new RuntimeException("fake")));

        repository.markDocumentEventsPublishedOrFailed(successes, failures);

        DocumentEventEntity publishedEntity = findDocumentEventEntityWhere(
                Query.withValue("status", Query.BinOp.eq, DocumentEventEntity.Status.published));
        DocumentEventEntity failedEntity = findDocumentEventEntityWhere(
                Query.withValue("status", Query.BinOp.eq, DocumentEventEntity.Status.failed));

        entity1.setStatus(DocumentEventEntity.Status.published);
        entity2.setStatus(DocumentEventEntity.Status.failed);
        entity1.setProcessedDate(ZonedDateTime.now(fixedClock));
        entity2.setProcessedDate(ZonedDateTime.now(fixedClock));

        assertEquals(entity1, publishedEntity);
        assertEquals(entity2, failedEntity);
    }

    @Test
    public void shouldThrowAwayEventsWhoseLockWasLostBeforeDocumentEventStatusUpdatesPersisted() throws Exception {
        insertDocumentEventEntities(randomNewDocumentEventEntities(20));

        // Sneakily steal away any acquired locks
        lockStrategy.allowLockButImmediateLoseIt();

        List<LightblueDocumentEvent> retrieved = repository.retrievePriorityDocumentEventsUpTo(10);

        assertThat(retrieved).isEmpty();
    }

    @Test
    public void shouldRecognizeUpdatesToProvidedConfiguration() throws Exception {
        insertDocumentEventEntities(
                newMultiStringDocumentEventEntity("1"),
                newMultiStringDocumentEventEntity("2"),
                newStringDocumentEventEntity("3"),
                newStringDocumentEventEntity("4"));

        config.setCanonicalTypesToProcess(Arrays.asList("String"));
        config.setDocumentEventsBatchSize(1);

        List<LightblueDocumentEvent> retrieved = repository.retrievePriorityDocumentEventsUpTo(4);

        assertThat(retrieved).hasSize(1);
        assertThat(retrieved.get(0).wrappedDocumentEventEntity().getParameterByKey("value")).isAnyOf("3", "4");
    }

    @Test(expected = Exception.class)
    public void shouldTreatExpiredDocumentEventsTransactionsAsInactive() throws Exception {
        // Slightly older than the expire threshold within the processing timeout.
        // If we timeout after 60 seconds, but we drop events within 20 seconds of that,
        // then this must be older than 40 seconds.
        Instant expiredDate = fixedClock.instant()
                .minus(PROCESSING_TIMEOUT)
                .plus(EXPIRE_THRESHOLD)
                .minus(Duration.ofMillis(1));

        LightblueDocumentEvent event = newDocumentEventThatStartedProcessingAt(expiredDate);

        repository.ensureTransactionActive(event);
    }

    @Test
    public void shouldTreatNotExpiredDocumentEventsTransactionsAsActive() throws Exception {
        // Slightly newer than the expire threshold within the processing timeout.
        // If we timeout after 60 seconds, but we drop events within 20 seconds of that,
        // then this must be newer than 40 seconds.
        Instant notExpiredDate = fixedClock.instant()
                .minus(PROCESSING_TIMEOUT)
                .plus(EXPIRE_THRESHOLD)
                .plus(Duration.ofMillis(1));

        LightblueDocumentEvent event = newDocumentEventThatStartedProcessingAt(notExpiredDate);

        repository.ensureTransactionActive(event);
    }

    @Test
    public void shouldRetrieveTimedOutDocumentEventsEvenThoughTheyAreProcessing() throws Exception {
        // Slightly older than the processing timeout.
        Instant timedout = fixedClock.instant()
                .minus(PROCESSING_TIMEOUT)
                .minus(Duration.ofMillis(1));

        LightblueDocumentEvent event = newDocumentEventThatStartedProcessingAt(timedout);

        insertDocumentEventEntities(event.wrappedDocumentEventEntity());

        assertThat(repository.retrievePriorityDocumentEventsUpTo(1)).hasSize(1);
    }

    @Test
    public void shouldNotRetrieveNotYetTimedOutProcessingDocumentEvents() throws Exception {
        // Slightly newer than the processing timeout.
        Instant notTimedOut = fixedClock.instant()
                .minus(PROCESSING_TIMEOUT)
                .plus(Duration.ofMillis(1));

        LightblueDocumentEvent event = newDocumentEventThatStartedProcessingAt(notTimedOut);

        insertDocumentEventEntities(event.wrappedDocumentEventEntity());

        assertThat(repository.retrievePriorityDocumentEventsUpTo(1)).isEmpty();
    }

    /**
     * This prevents against the following unlikely but possible race condition:
     * <ol>
     *     <li>Thread 1 retrieves expired events</li>
     *     <li>Thread 2 retrieves expired events</li>
     *     <li>Thread 1 locks expired events</li>
     *     <li>Thread 1 updates expired events timestamp</li>
     *     <li>Thread 1 releases lock on expired events and returns</li>
     *     <li>Thread 2 locks expired events</li>
     *     <li>Thread 2 updates expired events timestamp</li>
     *     <li>Thread 2 releases lock on expired events and returns</li>
     * </ol>
     *
     * This would result in the same event being processed twice. The test makes sure that when
     * thread 2 tries to update the expired events timestamp, it sees that it is modified and does
     * not return the event.
     */
    @Test
    public void shouldIgnoreRetrievedExpiredEventsUpdatedByAnotherThreadBeforeTimestampCanBeUpdated()
            throws Exception {
        lockStrategy.pauseAfterLock();

        // Slightly older than the processing timeout.
        Instant timedout = fixedClock.instant()
                .minus(PROCESSING_TIMEOUT)
                .minus(Duration.ofMillis(1));

        LightblueDocumentEvent event = newDocumentEventThatStartedProcessingAt(timedout);

        DocumentEventEntity inserted =
                insertDocumentEventEntities(event.wrappedDocumentEventEntity())[0];

        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            Future<List<LightblueDocumentEvent>> futureEvents =
                    executor.submit(() -> repository.retrievePriorityDocumentEventsUpTo(1));

            lockStrategy.waitForLock();

            // Update event timestamp while lock strategy is paused
            inserted.setProcessingDate(ZonedDateTime.now(fixedClock).plus(1, ChronoUnit.SECONDS));
            saveDocumentEventEntity(inserted);

            lockStrategy.unpause();

            assertEquals(0, futureEvents.get().size());
        } finally {
            executor.shutdown();
            try {
                executor.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }
    }

    @Test(expected = Exception.class)
    public void shouldRecognizeUpdatesToProvidedTimeoutsConfiguration() throws Exception {
        Duration newProcessingTimeout = PROCESSING_TIMEOUT.dividedBy(2);
        Duration newExpireThreshold = EXPIRE_THRESHOLD.dividedBy(2);

        config.setDocumentEventProcessingTimeout(newProcessingTimeout);
        config.setDocumentEventExpireThreshold(newExpireThreshold);

        Instant expiredDate = fixedClock.instant()
                .minus(newProcessingTimeout)
                .plus(newExpireThreshold)
                .minus(Duration.ofMillis(1));

        LightblueDocumentEvent event = newDocumentEventThatStartedProcessingAt(expiredDate);

        repository.ensureTransactionActive(event);
    }

    @Test
    public void shouldRetrieveOldestEventsOfSamePriorityFirst() throws Exception {
        ZonedDateTime now = ZonedDateTime.now(fixedClock);

        insertDocumentEventEntities(
                newDocumentEventEntityCreatedAt("-1", now.minus(1, ChronoUnit.MINUTES)),
                newDocumentEventEntityCreatedAt("-9", now.minus(9, ChronoUnit.MINUTES)),
                newDocumentEventEntityCreatedAt("-5", now.minus(5, ChronoUnit.MINUTES)),
                newDocumentEventEntityCreatedAt("-3", now.minus(3, ChronoUnit.MINUTES)),
                newDocumentEventEntityCreatedAt("-8", now.minus(8, ChronoUnit.MINUTES)),
                newDocumentEventEntityCreatedAt("-12", now.minus(12, ChronoUnit.MINUTES)),
                newDocumentEventEntityCreatedAt("-2", now.minus(2, ChronoUnit.MINUTES)));

        List<String> values = repository.retrievePriorityDocumentEventsUpTo(5).stream()
                .map(LightblueDocumentEvent::wrappedDocumentEventEntity)
                .map(e -> e.getParameterByKey("value"))
                .collect(Collectors.toList());

        assertThat(values).containsExactly("-12", "-8", "-9", "-5", "-3");
    }

    @Test
    public void shouldAllowReprocessingARecentlyProcessedEvent() throws Exception {
        // Slightly newer than the processing timeout.
        Instant notTimedOut = fixedClock.instant()
                .minus(PROCESSING_TIMEOUT)
                .plus(Duration.ofMillis(1));

        LightblueDocumentEvent event = newDocumentEventPublishedAt(notTimedOut);

        // Manually unprocess the event but don't reset dates
        event.wrappedDocumentEventEntity().setStatus(DocumentEventEntity.Status.unprocessed);

        repository.addNewDocumentEvents(Collections.singleton(event));

        assertEquals(1, repository.retrievePriorityDocumentEventsUpTo(10).size());
    }

    @Test
    public void shouldReturnEventsWhichFailedToParse() throws Exception {
        DocumentEventEntity gonnaFail = DocumentEventEntity.newlyCreated(null, "Fails", 50,
                ZonedDateTime.now(fixedClock),
                new DocumentEventEntity.KeyAndValue("bogus", "value"));

        insertDocumentEventEntities(gonnaFail);

        List<LightblueDocumentEvent> retrieved = repository.retrievePriorityDocumentEventsUpTo(1);

        assertEquals(1, retrieved.size());

        expectedException.expectCause(CoreMatchers.instanceOf(RuntimeException.class));

        retrieved.get(0).lookupDocument().get();
    }

    private List<DocumentEventEntity> findDocumentEventEntitiesWhere(@Nullable Query query)
            throws LightblueException {
        DataFindRequest find = new DataFindRequest(
                DocumentEventEntity.ENTITY_NAME,
                DocumentEventEntity.VERSION);
        find.where(query);
        find.select(Projection.includeFieldRecursively("*"));
        return Arrays.asList(client.data(find, DocumentEventEntity[].class));
    }

    private DocumentEventEntity findDocumentEventEntityWhere(@Nullable Query query)
            throws LightblueException {
        DataFindRequest find = new DataFindRequest(
                DocumentEventEntity.ENTITY_NAME,
                DocumentEventEntity.VERSION);
        find.where(query);
        find.select(Projection.includeFieldRecursively("*"));

        DocumentEventEntity[] found = client.data(find, DocumentEventEntity[].class);

        assertThat(found).hasLength(1);

        return found[0];
    }

    private DocumentEventEntity newMultiStringDocumentEventEntity(String... values) {
        return new MultiStringDocumentEvent(null, Arrays.asList(values), fixedClock)
                .wrappedDocumentEventEntity();
    }

    private DocumentEventEntity newStringDocumentEventEntity(String value) {
        return new StringDocumentEvent(null, value, fixedClock).wrappedDocumentEventEntity();
    }

    private DocumentEventEntity newStringDocumentEventEntity(String value, Clock clock) {
        return new StringDocumentEvent(null, value, clock).wrappedDocumentEventEntity();
    }

    private DocumentEventEntity newRandomStringDocumentEventEntityWithPriorityOverride(int priority) {
        DocumentEventEntity entity = new StringDocumentEvent(null, UUID.randomUUID().toString(), fixedClock)
                .wrappedDocumentEventEntity();
        entity.setPriority(priority);
        return entity;
    }

    private DocumentEventEntity newDocumentEventEntityCreatedAt(String value, ZonedDateTime creationDate) {
        DocumentEventEntity entity = newStringDocumentEventEntity(value);
        entity.setCreationDate(creationDate);
        return entity;
    }

    private LightblueDocumentEvent newDocumentEventThatStartedProcessingAt(Instant processingDate) {
        LightblueDocumentEvent event = new StringDocumentEvent(null, "processing", fixedClock);
        DocumentEventEntity expiredEntity = event.wrappedDocumentEventEntity();
        expiredEntity.setStatus(DocumentEventEntity.Status.processing);
        expiredEntity.setProcessingDate(ZonedDateTime.ofInstant(processingDate, fixedClock.getZone()));
        return event;
    }

    private LightblueDocumentEvent newDocumentEventPublishedAt(Instant publishedDate) {
        LightblueDocumentEvent event = new StringDocumentEvent(null, "published", fixedClock);
        DocumentEventEntity expiredEntity = event.wrappedDocumentEventEntity();
        expiredEntity.setStatus(DocumentEventEntity.Status.published);
        expiredEntity.setProcessingDate(ZonedDateTime.ofInstant(publishedDate.minus(1, ChronoUnit.SECONDS), fixedClock.getZone()));
        expiredEntity.setProcessedDate(ZonedDateTime.ofInstant(publishedDate, fixedClock.getZone()));
        return event;
    }

    private Collection<StringDocumentEvent> randomNewStringDocumentEvents(int amount) {
        return Arrays.stream(randomNewDocumentEventEntities(amount))
                .map(StringDocumentEvent::new)
                .collect(Collectors.toList());
    }

    private DocumentEventEntity[] randomNewDocumentEventEntities(int amount) {
        DocumentEventEntity[] entities = new DocumentEventEntity[amount];

        for (int i = 0; i < amount; i++) {
            entities[i] = newStringDocumentEventEntity(Integer.toString(i));
        }

        return entities;
    }

    private DocumentEventEntity[] insertDocumentEventEntities(DocumentEventEntity... entities) throws LightblueException {
        DataInsertRequest insertEntities = new DataInsertRequest(
                DocumentEventEntity.ENTITY_NAME, DocumentEventEntity.VERSION);
        insertEntities.create(entities);
        insertEntities.returns(Projection.includeFieldRecursively("*"));
        return client.data(insertEntities, DocumentEventEntity[].class);
    }

    /** Orders the entities by priority */
    private void insertDocumentEventEntitiesInOrder(DocumentEventEntity... entities) throws LightblueException {
        DataInsertRequest insertEntities = new DataInsertRequest(
                DocumentEventEntity.ENTITY_NAME, DocumentEventEntity.VERSION);

        int startPriority = 50;

        for (DocumentEventEntity entity : entities) {
            entity.setPriority(startPriority--);
        }

        insertEntities.create(entities);
        client.data(insertEntities);
    }

    private void saveDocumentEventEntity(DocumentEventEntity entity) throws LightblueException {
        DataSaveRequest save = new DataSaveRequest(DocumentEventEntity.ENTITY_NAME, DocumentEventEntity.VERSION);
        save.create(entity);
        client.data(save);
    }
}
