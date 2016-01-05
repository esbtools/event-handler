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
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.integration.test.LightblueExternalResource;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.DataInsertRequest;
import com.redhat.lightblue.client.response.LightblueException;

import org.esbtools.eventhandler.lightblue.model.DocumentEventEntity;
import org.esbtools.eventhandler.lightblue.testing.EmptyNotificationFactory;
import org.esbtools.eventhandler.lightblue.testing.LightblueClientConfigurations;
import org.esbtools.eventhandler.lightblue.testing.LightblueClients;
import org.esbtools.eventhandler.lightblue.testing.SlowDataLightblueClient;
import org.esbtools.eventhandler.lightblue.testing.StringDocumentEvent;
import org.esbtools.eventhandler.lightblue.testing.MultiStringDocumentEvent;
import org.esbtools.eventhandler.lightblue.testing.TestMetadataJson;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

    private LightblueClient client;

    private LightblueEventRepository repository;

    private Clock fixedClock = Clock.fixed(Instant.now(), ZoneOffset.UTC);

    private static final int DOCUMENT_EVENT_BATCH_SIZE = 10;

    @Before
    public void initializeLightblueClientAndRepository() {
        LightblueClientConfiguration config = LightblueClientConfigurations
                .fromLightblueExternalResource(lightblueExternalResource);
        client = LightblueClients.withJavaTimeSerializationSupport(config);

        repository = new LightblueEventRepository(client, new String[]{"String", "Strings"},
                DOCUMENT_EVENT_BATCH_SIZE, "testLockingDomain", new EmptyNotificationFactory(),
                new ByTypeDocumentEventFactory()
                        .addType("String", StringDocumentEvent::new)
                        .addType("Strings", MultiStringDocumentEvent::new),
                fixedClock);
    }

    @Before
    public void dropDocEventEntities() throws UnknownHostException {
        lightblueExternalResource.cleanupMongoCollections(DocumentEventEntity.ENTITY_NAME);
    }

    @Test
    public void shouldRetrieveDocumentEventsForSpecifiedEntities() throws LightblueException {
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
    public void shouldMarkRetrievedDocumentEventsAsProcessing() throws LightblueException {
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
    public void shouldOnlyRetrieveUnprocessedEvents() throws LightblueException {
        DocumentEventEntity stringEvent = newStringDocumentEventEntity("right");
        DocumentEventEntity processingEvent = newStringDocumentEventEntity("wrong");
        processingEvent.setStatus(DocumentEventEntity.Status.processing);
        DocumentEventEntity processedEvent = newStringDocumentEventEntity("wrong");
        processedEvent.setStatus(DocumentEventEntity.Status.processed);
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

        LightblueEventRepository thread1Repository = new LightblueEventRepository(
                thread1Client, new String[]{"String"}, 100,
                "testLockingDomain", new EmptyNotificationFactory(),
                new ByTypeDocumentEventFactory().addType("String", StringDocumentEvent::new),
                fixedClock);
        LightblueEventRepository thread2Repository = new LightblueEventRepository(
                thread2Client, new String[]{"String"}, 100,
                "testLockingDomain", new EmptyNotificationFactory(),
                new ByTypeDocumentEventFactory().addType("String", StringDocumentEvent::new),
                fixedClock);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            insertDocumentEventEntities(randomNewDocumentEventEntities(100));

            CountDownLatch bothThreadsStarted = new CountDownLatch(2);

            thread1Client.pauseOnNextRequest();
            thread2Client.pauseOnNextRequest();

            Future<List<LightblueDocumentEvent>> futureThread1Events = executor.submit(() -> {
                bothThreadsStarted.countDown();
                return thread1Repository.retrievePriorityDocumentEventsUpTo(100);
            });
            Future<List<LightblueDocumentEvent>> futureThread2Events = executor.submit(() -> {
                bothThreadsStarted.countDown();
                return thread2Repository.retrievePriorityDocumentEventsUpTo(100);
            });

            bothThreadsStarted.await();

            Thread.sleep(5000);

            thread2Client.unpause();
            thread1Client.unpause();

            List<LightblueDocumentEvent> thread1Events = futureThread1Events.get(5, TimeUnit.SECONDS);
            List<LightblueDocumentEvent> thread2Events = futureThread2Events.get(5, TimeUnit.SECONDS);

            if (thread1Events.isEmpty()) {
                assertEquals("Either both threads got events, or some events weren't retrieved.",
                        100, thread2Events.size());
            } else {
                assertEquals("Either both threads got events, or some events weren't retrieved.",
                        100, thread1Events.size());
                assertEquals("Both threads got events!", 0, thread2Events.size());
            }
        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    @Test
    public void shouldRetrieveDocumentEventsInPriorityOrder() throws LightblueException {
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
    public void shouldIgnoreSupersededEventsAndMarkAsSupersededAndTrackVictimIds() throws LightblueException {
        Clock creationTimeClock = Clock.offset(fixedClock, Duration.ofHours(1).negated());

        insertDocumentEventEntities(
                newStringDocumentEventEntity("duplicate", creationTimeClock),
                newStringDocumentEventEntity("duplicate", creationTimeClock),
                newStringDocumentEventEntity("duplicate", creationTimeClock),
                newStringDocumentEventEntity("duplicate", creationTimeClock),
                newStringDocumentEventEntity("duplicate", creationTimeClock));

        List<LightblueDocumentEvent> retrieved = repository.retrievePriorityDocumentEventsUpTo(5);

        assertEquals(1, retrieved.size());

        DataFindRequest findSuperseded = new DataFindRequest(DocumentEventEntity.ENTITY_NAME,
                DocumentEventEntity.VERSION);
        findSuperseded.select(Projection.includeFieldRecursively("*"));
        findSuperseded.where(Query.withValue("status", Query.BinOp.eq, DocumentEventEntity.Status.superseded));

        DocumentEventEntity[] found = client.data(findSuperseded, DocumentEventEntity[].class);

        Instant processedDate = ZonedDateTime.now(fixedClock).toInstant();

        assertEquals(
                Arrays.asList(processedDate, processedDate, processedDate, processedDate),
                Arrays.stream(found)
                        .map(DocumentEventEntity::getProcessedDate)
                        .map(ZonedDateTime::toInstant)
                        .collect(Collectors.toList()));

        DocumentEventEntity survivorEntity = retrieved.get(0).wrappedDocumentEventEntity();

        assertThat(survivorEntity.getSurvivorOfIds()).containsExactlyElementsIn(
                Arrays.stream(found).map(DocumentEventEntity::get_id).collect(Collectors.toList()));
    }

    @Test
    public void shouldMergeEventsInBatchAndMarkAsMergedAndTrackVictims() throws LightblueException {
        insertDocumentEventEntities(
                newMultiStringDocumentEventEntity("1"),
                newMultiStringDocumentEventEntity("2"),
                newMultiStringDocumentEventEntity("3"),
                newMultiStringDocumentEventEntity("4"),
                newMultiStringDocumentEventEntity("5"));

        List<LightblueDocumentEvent> retrieved = repository.retrievePriorityDocumentEventsUpTo(5);

        assertThat(retrieved).hasSize(1);

        List<DocumentEventEntity> mergedEntities = findDocumentEventEntitiesWhere(
                Query.withValue("status", Query.BinOp.eq, DocumentEventEntity.Status.merged));

        DocumentEventEntity retrievedEntity = retrieved.get(0).wrappedDocumentEventEntity();

        assertThat(mergedEntities).named("merged entities").hasSize(5);
        assertThat(retrievedEntity.getSurvivorOfIds())
                .containsExactlyElementsIn(
                mergedEntities.stream()
                        .map(DocumentEventEntity::get_id)
                        .collect(Collectors.toList()));
    }

    @Test
    public void shouldPersistMergerEntitiesAsProcessingIfCreatedDuringRetrieval()
            throws LightblueException {
        insertDocumentEventEntities(
                newMultiStringDocumentEventEntity("1"),
                newMultiStringDocumentEventEntity("2"),
                newMultiStringDocumentEventEntity("3"),
                newMultiStringDocumentEventEntity("4"),
                newMultiStringDocumentEventEntity("5"));

        List<LightblueDocumentEvent> retrieved = repository.retrievePriorityDocumentEventsUpTo(5);
        DocumentEventEntity retrievedEntity = retrieved.get(0).wrappedDocumentEventEntity();

        List<DocumentEventEntity> processingEntities = findDocumentEventEntitiesWhere(
                Query.withValue("status", Query.BinOp.eq, DocumentEventEntity.Status.processing));

        assertThat(processingEntities).named("persisted processing entities").hasSize(1);
        assertThat(retrievedEntity.get_id()).named("retrieved event id")
                .isEqualTo(processingEntities.get(0).get_id());
    }

    @Test
    public void shouldReturnNoMoreThanMaxEventsDespiteBatchSizeAndLeaveRemainingEventsUnprocessed() throws LightblueException {
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
    public void shouldSearchThroughNoMoreThanBatchSize() throws LightblueException {
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
            throws LightblueException {
        StringDocumentEvent event1 = new StringDocumentEvent("1", fixedClock);
        StringDocumentEvent event2 = new StringDocumentEvent("2", fixedClock);
        StringDocumentEvent event3 = new StringDocumentEvent("3", fixedClock);
        StringDocumentEvent event4 = new StringDocumentEvent("4", fixedClock);

        // 4 which are not able to be merged, and 7 which can be.
        // Batch size should be 10, so one of these will remain unprocessed.
        insertDocumentEventEntities(
                event1.wrappedDocumentEventEntity(),
                event2.wrappedDocumentEventEntity(),
                event3.wrappedDocumentEventEntity(),
                event4.wrappedDocumentEventEntity(),
                newMultiStringDocumentEventEntity("should merge 1"),
                newMultiStringDocumentEventEntity("should merge 2"),
                newMultiStringDocumentEventEntity("should merge 3"),
                newMultiStringDocumentEventEntity("should merge 4"),
                newMultiStringDocumentEventEntity("should merge 5"),
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
        assertThat(returnedStringEvents).hasSize(4);
        assertThat(returnedMultiStringEvents).hasSize(1);
        assertThat(returnedMultiStringEvents.get(0).values()).isEqualTo(Arrays.asList(
                "should merge 1", "should merge 2", "should merge 3", "should merge 4",
                "should merge 5", "should merge 6"));
    }

    private List<DocumentEventEntity> findDocumentEventEntitiesWhere(Query query)
            throws LightblueException {
        DataFindRequest find = new DataFindRequest(
                DocumentEventEntity.ENTITY_NAME,
                DocumentEventEntity.VERSION);
        find.where(query);
        find.select(Projection.includeFieldRecursively("*"));
        return Arrays.asList(client.data(find, DocumentEventEntity[].class));
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
            entities[i] = newStringDocumentEventEntity(UUID.randomUUID().toString());
        }

        return entities;
    }

    private void insertDocumentEventEntities(DocumentEventEntity... entities) throws LightblueException {
        DataInsertRequest insertEntities = new DataInsertRequest(
                DocumentEventEntity.ENTITY_NAME, DocumentEventEntity.VERSION);
        insertEntities.create(entities);
        client.data(insertEntities);
    }
}
