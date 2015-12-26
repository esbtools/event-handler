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

import org.esbtools.eventhandler.DocumentEvent;
import org.esbtools.eventhandler.lightblue.model.DocumentEventEntity;
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

    private LightblueClient client;

    private LightblueEventRepository repository;

    private Clock fixedClock = Clock.fixed(Instant.now(), ZoneOffset.UTC);

    @Before
    public void initializeLightblueClientAndRepository() {
        LightblueClientConfiguration config = LightblueClientConfigurations
                .fromLightblueExternalResource(lightblueExternalResource);
        client = LightblueClients.withJavaTimeSerializationSupport(config);

        repository = new LightblueEventRepository(client, new String[]{"String"}, 10,
                "testLockingDomain", new EmptyNotificationFactory(),
                new ByTypeDocumentEventFactory().addType("String", StringDocumentEvent::new),
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

        List<DocumentEvent> docEvents = repository.retrievePriorityDocumentEventsUpTo(2);

        assertEquals(1, docEvents.size());

        DocumentEventEntity entity = ((LightblueDocumentEvent) docEvents.get(0))
                .wrappedDocumentEventEntity()
                .get();

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

        List<DocumentEvent> retrieved = repository.retrievePriorityDocumentEventsUpTo(6);

        assertEquals(1, retrieved.size());
        assertEquals("right", ((LightblueDocumentEvent) retrieved.get(0))
                .wrappedDocumentEventEntity()
                .get()
                .getParameterByKey("value"));
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

            Future<List<DocumentEvent>> futureThread1Events = executor.submit(() -> {
                bothThreadsStarted.countDown();
                return thread1Repository.retrievePriorityDocumentEventsUpTo(100);
            });
            Future<List<DocumentEvent>> futureThread2Events = executor.submit(() -> {
                bothThreadsStarted.countDown();
                return thread2Repository.retrievePriorityDocumentEventsUpTo(100);
            });

            bothThreadsStarted.await();

            Thread.sleep(5000);

            thread2Client.unpause();
            thread1Client.unpause();

            List<DocumentEvent> thread1Events = futureThread1Events.get(5, TimeUnit.SECONDS);
            List<DocumentEvent> thread2Events = futureThread2Events.get(5, TimeUnit.SECONDS);

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
                .map(d -> (LightblueDocumentEvent) d)
                .map(LightblueDocumentEvent::wrappedDocumentEventEntity)
                .map(Optional::get)
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

        List<DocumentEvent> retrieved = repository.retrievePriorityDocumentEventsUpTo(5);

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

        DocumentEventEntity survivorEntity = ((LightblueDocumentEvent) retrieved.get(0))
                .wrappedDocumentEventEntity().get();

        assertThat(survivorEntity.getSurvivorOfIds()).containsExactlyElementsIn(
                Arrays.stream(found).map(DocumentEventEntity::get_id).collect(Collectors.toList()));
    }

    private DocumentEventEntity newStringDocumentEventEntity(String value) {
        return new StringDocumentEvent(value, fixedClock).toNewDocumentEventEntity();
    }

    private DocumentEventEntity newStringDocumentEventEntity(String value, Clock clock) {
        return new StringDocumentEvent(value, clock).toNewDocumentEventEntity();
    }

    private DocumentEventEntity newRandomStringDocumentEventEntityWithPriorityOverride(int priority) {
        DocumentEventEntity entity = new StringDocumentEvent(UUID.randomUUID().toString(), fixedClock)
                .toNewDocumentEventEntity();
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
