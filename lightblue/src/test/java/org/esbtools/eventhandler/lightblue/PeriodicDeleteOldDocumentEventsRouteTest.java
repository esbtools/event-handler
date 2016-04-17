/*
 *  Copyright 2016 esbtools Contributors and/or its affiliates.
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

import org.esbtools.eventhandler.lightblue.testing.InMemoryLockStrategy;
import org.esbtools.eventhandler.lightblue.testing.LightblueClientConfigurations;
import org.esbtools.eventhandler.lightblue.testing.LightblueClients;
import org.esbtools.eventhandler.lightblue.testing.StringDocumentEvent;
import org.esbtools.eventhandler.lightblue.testing.TestMetadataJson;

import com.google.common.truth.Truth;
import com.jayway.awaitility.Awaitility;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueClientConfiguration;
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.integration.test.LightblueExternalResource;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.DataInsertRequest;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class PeriodicDeleteOldDocumentEventsRouteTest extends CamelTestSupport {
    @ClassRule
    public static LightblueExternalResource lightblueExternalResource =
            new LightblueExternalResource(TestMetadataJson.forEntity(DocumentEventEntity.class));

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    LightblueClient client;
    Clock fixedClock = Clock.fixed(Instant.now(), ZoneId.of("GMT"));
    InMemoryLockStrategy lockStrategy = new InMemoryLockStrategy();

    static final Duration DELETE_EVENTS_OLDER_THAN = Duration.ofHours(1);
    static final Duration DELETE_INTERVAL = Duration.ofSeconds(2);

    @Before
    public void dropDocEventEntities() throws UnknownHostException {
        lightblueExternalResource.cleanupMongoCollections(DocumentEventEntity.ENTITY_NAME);
    }

    @Override
    public void doPreSetup() {
        LightblueClientConfiguration lbClientConfig = LightblueClientConfigurations
                .fromLightblueExternalResource(lightblueExternalResource);
        client = LightblueClients.withJavaTimeSerializationSupport(lbClientConfig);

        // Reset locks, start with lock taken
        lockStrategy.releaseAll();
        lockStrategy.forceAcquire(PeriodicDeleteOldDocumentEventsRoute.DELETER_LOCK_RESOURCE_ID);
    }

    @Override
    protected RoutesBuilder createRouteBuilder() throws Exception {
        return new PeriodicDeleteOldDocumentEventsRoute(client, lockStrategy,
                DELETE_EVENTS_OLDER_THAN, DELETE_INTERVAL, fixedClock);
    }

    @Test
    public void shouldDeleteOldDocumentEventsIfCanAcquireLock() throws Exception {
        lockStrategy.releaseAll();

        Instant tooOld = fixedClock.instant()
                .minus(DELETE_EVENTS_OLDER_THAN)
                .minus(1, ChronoUnit.SECONDS);

        Instant notOldEnough = fixedClock.instant()
                .minus(DELETE_EVENTS_OLDER_THAN)
                .plus(1, ChronoUnit.SECONDS);

        insertDocumentEventsCreatedAt(tooOld, 10);
        insertDocumentEventsCreatedAt(notOldEnough, 5);

        List<DocumentEventEntity> fiveRemaining =
                Awaitility.await().until(this::findAllDocumentEvents, Matchers.hasSize(5));

        Truth.assertThat(fiveRemaining.stream()
                .map(DocumentEventEntity::getCreationDate)
                .map(ZonedDateTime::toInstant)
                .distinct()
                .collect(Collectors.toList()))
                .containsExactly(notOldEnough);
    }

    @Test
    public void shouldNotDeleteOldDocumentEventsIfCannotAcquireLock() throws Exception {
        Instant tooOld = fixedClock.instant()
                .minus(DELETE_EVENTS_OLDER_THAN)
                .minus(1, ChronoUnit.SECONDS);

        insertDocumentEventsCreatedAt(tooOld, 10);

        Thread.sleep(DELETE_INTERVAL.multipliedBy(3).toMillis());

        Truth.assertThat(findAllDocumentEvents()).hasSize(10);
    }

    @Test
    public void shouldStopDeletingDocumentEventsIfLostLock() throws Exception {
        lockStrategy.releaseAll();

        Instant tooOld = fixedClock.instant()
                .minus(DELETE_EVENTS_OLDER_THAN)
                .minus(1, ChronoUnit.SECONDS);

        insertDocumentEventsCreatedAt(tooOld, 10);

        Awaitility.await("Initial delete once route acquires lock")
                .until(this::findAllDocumentEvents, Matchers.hasSize(0));

        // Now let's steal lock...
        lockStrategy.forceAcquire(PeriodicDeleteOldDocumentEventsRoute.DELETER_LOCK_RESOURCE_ID);

        // Make sure any delete in progress finishes by waiting for the next attempt to acquire lock
        // which should fail.
        lockStrategy.waitForTryAcquire();

        insertDocumentEventsCreatedAt(tooOld, 10);

        Thread.sleep(DELETE_INTERVAL.multipliedBy(3).toMillis());

        Truth.assertThat(findAllDocumentEvents()).hasSize(10);
    }

    List<DocumentEventEntity> findAllDocumentEvents() throws LightblueException {
        DataFindRequest find = new DataFindRequest(
                DocumentEventEntity.ENTITY_NAME,
                DocumentEventEntity.VERSION);

        find.select(Projection.includeFieldRecursively("*"));

        DocumentEventEntity[] found = client.data(find, DocumentEventEntity[].class);

        return Arrays.asList(found);
    }

    void insertDocumentEventsCreatedAt(Instant creationDate, int numberOfEvents)
            throws LightblueException {
        for (int i = 0; i < numberOfEvents; i++) {
            StringDocumentEvent event = new StringDocumentEvent(null, UUID.randomUUID().toString(),
                    Clock.fixed(creationDate, fixedClock.getZone()));

            DocumentEventEntity entity = event.wrappedDocumentEventEntity();

            DataInsertRequest insert = new DataInsertRequest(
                    DocumentEventEntity.ENTITY_NAME,
                    DocumentEventEntity.VERSION);
            insert.create(entity);

            client.data(insert);
        }
    }
}
