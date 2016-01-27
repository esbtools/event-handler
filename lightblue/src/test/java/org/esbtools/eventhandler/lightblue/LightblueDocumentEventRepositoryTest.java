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

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueClientConfiguration;
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.integration.test.LightblueExternalResource;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.DataInsertRequest;

import org.esbtools.eventhandler.FailedDocumentEvent;
import org.esbtools.eventhandler.lightblue.model.DocumentEventEntity;
import org.esbtools.eventhandler.lightblue.testing.InMemoryLockStrategy;
import org.esbtools.eventhandler.lightblue.testing.LightblueClientConfigurations;
import org.esbtools.eventhandler.lightblue.testing.LightblueClients;
import org.esbtools.eventhandler.lightblue.testing.MultiStringDocumentEvent;
import org.esbtools.eventhandler.lightblue.testing.SlowDataLightblueClient;
import org.esbtools.eventhandler.lightblue.testing.StringDocumentEvent;
import org.esbtools.eventhandler.lightblue.testing.TestMetadataJson;
import org.hamcrest.Matchers;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private LightblueClient client;

    private LightblueDocumentEventRepository repository;

    private Clock fixedClock = Clock.fixed(Instant.now(), ZoneId.of("GMT"));

    private static final int DOCUMENT_EVENT_BATCH_SIZE = 10;

    private Map<String, DocumentEventFactory> documentEventFactoriesByType =
            new HashMap<String, DocumentEventFactory>() {{
                put("String", StringDocumentEvent::new);
                put("MultiString", MultiStringDocumentEvent::new);
            }};

    @Before
    public void initializeLightblueClientAndRepository() {
        LightblueClientConfiguration config = LightblueClientConfigurations
                .fromLightblueExternalResource(lightblueExternalResource);
        client = LightblueClients.withJavaTimeSerializationSupport(config);

        // TODO: Try and reduce places canonical types are specified
        // We have 3 here: type list to process, types to factories, and inside the doc event impls
        // themselves.
        repository = new LightblueDocumentEventRepository(client, new String[]{"String", "MultiString"},
                DOCUMENT_EVENT_BATCH_SIZE, new InMemoryLockStrategy(), documentEventFactoriesByType,
                fixedClock);
    }

    @Before
    public void dropDocEventEntities() throws UnknownHostException {
        lightblueExternalResource.cleanupMongoCollections(DocumentEventEntity.ENTITY_NAME);
    }

    @Test
    public void shouldRetrieveDocumentEventsForSpecifiedEntities() throws Exception {
        DocumentEventEntity stringEvent = newStringDocumentEventEntity("foo");

        DocumentEventEntity otherEvent = DocumentEventEntity.newlyCreated("Other", 50,
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
    public void shouldReturnNonOverlappingSetsEvenAmongMultipleThreads() throws LightblueException,
            InterruptedException, ExecutionException, TimeoutException {
        SlowDataLightblueClient thread1Client = new SlowDataLightblueClient(client);
        SlowDataLightblueClient thread2Client = new SlowDataLightblueClient(client);

        LightblueDocumentEventRepository thread1Repository = new LightblueDocumentEventRepository(
                thread1Client, new String[]{"String"}, 50, new InMemoryLockStrategy(),
                documentEventFactoriesByType, fixedClock);
        LightblueDocumentEventRepository thread2Repository = new LightblueDocumentEventRepository(
                thread2Client, new String[]{"String"}, 50, new InMemoryLockStrategy(),
                documentEventFactoriesByType, fixedClock);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            DocumentEventEntity[] entities = randomNewDocumentEventEntities(20);

            List<String> expectedValues = Arrays.stream(entities)
                    .map(e -> e.getParameterByKey("value"))
                    .collect(Collectors.toList());

            insertDocumentEventEntities(entities);

            CountDownLatch bothThreadsStarted = new CountDownLatch(2);

            thread1Client.pauseOnNextRequest();
            thread2Client.pauseOnNextRequest();

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

            List<LightblueDocumentEvent> thread1Events = futureThread1Events.get(5, TimeUnit.SECONDS);
            List<LightblueDocumentEvent> thread2Events = futureThread2Events.get(5, TimeUnit.SECONDS);

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
    public void shouldRetrieveDocumentEventsInPriorityOrder() throws Exception {
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

        List<Integer> priorities = repository.retrievePriorityDocumentEventsUpTo(10).stream()
                .map(LightblueDocumentEvent::wrappedDocumentEventEntity)
                .map(DocumentEventEntity::getPriority)
                .collect(Collectors.toList());

        assertEquals(Arrays.asList(100, 99, 70, 55, 50, 30, 25, 25, 5, 2), priorities);
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

        DataFindRequest findSuperseded = new DataFindRequest(DocumentEventEntity.ENTITY_NAME,
                DocumentEventEntity.VERSION);
        findSuperseded.select(Projection.includeFieldRecursively("*"));
        findSuperseded.where(Query.withValue("status", Query.BinOp.eq, DocumentEventEntity.Status.superseded));

        DocumentEventEntity[] supersededEntities = client.data(findSuperseded, DocumentEventEntity[].class);

        assertEquals(1, retrieved.size());
        assertEquals(
                Arrays.asList(expectedProcessedDate, expectedProcessedDate, expectedProcessedDate, expectedProcessedDate),
                Arrays.stream(supersededEntities)
                        .map(DocumentEventEntity::getProcessedDate)
                        .map(ZonedDateTime::toInstant)
                        .collect(Collectors.toList()));

        DocumentEventEntity survivorEntity = retrieved.get(0).wrappedDocumentEventEntity();

        assertThat(survivorEntity.getSurvivorOfIds()).containsExactlyElementsIn(
                Arrays.stream(supersededEntities)
                        .map(DocumentEventEntity::get_id)
                        .collect(Collectors.toList()));
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

        assertThat(retrieved).hasSize(1);
        assertThat(mergedEntities).named("merged entities").hasSize(5);

        DocumentEventEntity retrievedEntity = retrieved.get(0).wrappedDocumentEventEntity();

        assertThat(retrievedEntity.getSurvivorOfIds())
                .containsExactlyElementsIn(
                mergedEntities.stream()
                        .map(DocumentEventEntity::get_id)
                        .collect(Collectors.toList()));
    }

    @Test
    public void shouldPersistMergerEntitiesAsProcessingAndUpdateRetrievedWithIdIfCreatedDuringRetrieval()
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
                newMultiStringDocumentEventEntity("should merge 6"),
                newStringDocumentEventEntity("4"),
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
        StringDocumentEvent event1 = new StringDocumentEvent("1", fixedClock);
        MultiStringDocumentEvent event2 = new MultiStringDocumentEvent(Arrays.asList("2"), fixedClock);

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
    public void shouldUpdateProcessedEntitiesWithStatusAndDatePostPublishing() throws LightblueException {
        Clock creationTimeClock = Clock.offset(fixedClock, Duration.ofHours(1).negated());

        StringDocumentEvent event1 = new StringDocumentEvent("1", creationTimeClock);
        StringDocumentEvent event2 = new StringDocumentEvent("2", creationTimeClock);
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

        repository.markDocumentEventsProcessedOrFailed(successes, failures);

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
    @Ignore("Behavior is different with new locking algorithm: should return with no exception and no events now")
    public void shouldThrowLostLockExceptionIfLockLostBeforeDocumentEventStatusUpdatesPersisted() throws Exception {
        SlowDataLightblueClient slowClient = new SlowDataLightblueClient(client);
        InMemoryLockStrategy lockStrategy = new InMemoryLockStrategy();

        ExecutorService executor = Executors.newSingleThreadExecutor();

        LightblueDocumentEventRepository repository = new LightblueDocumentEventRepository(client,
                new String[]{"String"}, 50, lockStrategy, documentEventFactoriesByType,
                Clock.systemUTC());

        slowClient.pauseOnNextRequest();

        insertDocumentEventEntities(randomNewDocumentEventEntities(20));

        try {
            // Sneakily steal away any acquired locks
            lockStrategy.allowLockButImmediateLoseIt();

            // We will block this task with the slow client; do it in another thread to avoid blocking
            // test.
            Future<?> futureDocEvents = executor.submit(() -> repository.retrievePriorityDocumentEventsUpTo(10));

            // This will cause processing to continue, which should notice the lock expired...
            slowClient.unpause();

            // ...throwing an exception.
            expectedException.expectCause(Matchers.instanceOf(LostLockException.class));

            futureDocEvents.get();
        } finally {
            executor.shutdownNow();
        }
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

    private DocumentEventEntity newMultiStringDocumentEventEntity(String value) {
        return new MultiStringDocumentEvent(Collections.singletonList(value), fixedClock)
                .wrappedDocumentEventEntity();
    }

    private DocumentEventEntity newStringDocumentEventEntity(String value) {
        return new StringDocumentEvent(value, fixedClock).wrappedDocumentEventEntity();
    }

    private DocumentEventEntity newStringDocumentEventEntity(String value, Clock clock) {
        return new StringDocumentEvent(value, clock).wrappedDocumentEventEntity();
    }

    private DocumentEventEntity newRandomStringDocumentEventEntityWithPriorityOverride(int priority) {
        DocumentEventEntity entity = new StringDocumentEvent(UUID.randomUUID().toString(), fixedClock)
                .wrappedDocumentEventEntity();
        entity.setPriority(priority);
        return entity;
    }

    private DocumentEventEntity[] randomNewDocumentEventEntities(int amount) {
        DocumentEventEntity[] entities = new DocumentEventEntity[amount];

        for (int i = 0; i < amount; i++) {
            entities[i] = newStringDocumentEventEntity(Integer.toString(i));
        }

        return entities;
    }

    private void insertDocumentEventEntities(DocumentEventEntity... entities) throws LightblueException {
        DataInsertRequest insertEntities = new DataInsertRequest(
                DocumentEventEntity.ENTITY_NAME, DocumentEventEntity.VERSION);
        insertEntities.create(entities);
        client.data(insertEntities);
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
}
