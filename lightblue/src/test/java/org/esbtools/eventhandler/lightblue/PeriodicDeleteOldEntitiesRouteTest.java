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
import org.esbtools.eventhandler.lightblue.testing.StringNotification;
import org.esbtools.eventhandler.lightblue.testing.TestMetadataJson;
import org.esbtools.lightbluenotificationhook.NotificationEntity;

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
import org.junit.Test;

import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PeriodicDeleteOldEntitiesRouteTest extends CamelTestSupport {
    @ClassRule
    public static LightblueExternalResource lightblueExternalResource = new LightblueExternalResource(
            TestMetadataJson.forEntities(DocumentEventEntity.class, NotificationEntity.class));

    LightblueClient client;
    Clock fixedClock = Clock.fixed(Instant.now(), ZoneId.of("GMT"));
    InMemoryLockStrategy lockStrategy = new InMemoryLockStrategy();

    PeriodicDeleteOldEntitiesRoute docEventsDeleterRoute;
    PeriodicDeleteOldEntitiesRoute notificationsDeleterRoute;

    static final Duration DELETE_OLDER_THAN = Duration.ofHours(1);
    static final Duration DELETE_INTERVAL = Duration.ofSeconds(2);

    @Before
    public void dropEntities() throws UnknownHostException {
        lightblueExternalResource.cleanupMongoCollections(
                DocumentEventEntity.ENTITY_NAME, NotificationEntity.ENTITY_NAME);
    }

    @Override
    public void doPreSetup() {
        LightblueClientConfiguration lbClientConfig = LightblueClientConfigurations
                .fromLightblueExternalResource(lightblueExternalResource);
        client = LightblueClients.withJavaTimeSerializationSupport(lbClientConfig);

        docEventsDeleterRoute = PeriodicDeleteOldEntitiesRoute
                .deletingDocumentEventsOlderThan(DELETE_OLDER_THAN, DELETE_INTERVAL,
                        client, lockStrategy, fixedClock);
        notificationsDeleterRoute = PeriodicDeleteOldEntitiesRoute
                .deletingNotificationsOlderThan(DELETE_OLDER_THAN, DELETE_INTERVAL,
                        client, lockStrategy, fixedClock);

        // Reset locks, start with lock taken
        lockStrategy.releaseAll();
        lockStrategy.forceAcquire(docEventsDeleterRoute.deleterLockResourceId);
        lockStrategy.forceAcquire(notificationsDeleterRoute.deleterLockResourceId);
    }

    @Override
    protected RoutesBuilder[] createRouteBuilders() throws Exception {
        return new PeriodicDeleteOldEntitiesRoute[]{docEventsDeleterRoute, notificationsDeleterRoute};
    }

    @Test
    public void shouldDeleteOldDocumentEventsAndNotificationsIfCanAcquireLocks() throws Exception {
        Instant tooOld = fixedClock.instant()
                .minus(DELETE_OLDER_THAN)
                .minus(1, ChronoUnit.SECONDS);

        Instant notOldEnough = fixedClock.instant()
                .minus(DELETE_OLDER_THAN)
                .plus(1, ChronoUnit.SECONDS);

        insertDocumentEventsCreatedAt(tooOld, 10);
        insertDocumentEventsCreatedAt(notOldEnough, 5);
        insertNotificationsForRequestsAt(tooOld, 10);
        insertNotificationsForRequestsAt(notOldEnough, 5);

        lockStrategy.releaseAll();

        List<DocumentEventEntity> fiveRemainingDocEvents =
                Awaitility.await().until(this::findAllDocumentEvents, Matchers.hasSize(5));

        List<NotificationEntity> fiveRemainingNotifications =
                Awaitility.await().until(this::findAllNotifications, Matchers.hasSize(5));

        Truth.assertThat(fiveRemainingDocEvents.stream()
                .map(DocumentEventEntity::getCreationDate)
                .map(ZonedDateTime::toInstant)
                .distinct()
                .collect(Collectors.toList()))
                .containsExactly(notOldEnough);

        Truth.assertThat(fiveRemainingNotifications.stream()
                .map(NotificationEntity::getClientRequestDate)
                .map(Date::toInstant)
                .distinct()
                .collect(Collectors.toList()))
                .containsExactly(notOldEnough);
    }

    @Test
    public void shouldNotDeleteOldDocumentEventsAndNotificationsIfCannotAcquireLock() throws Exception {
        Instant tooOld = fixedClock.instant()
                .minus(DELETE_OLDER_THAN)
                .minus(1, ChronoUnit.SECONDS);

        insertDocumentEventsCreatedAt(tooOld, 10);
        insertNotificationsForRequestsAt(tooOld, 10);

        Thread.sleep(DELETE_INTERVAL.multipliedBy(3).toMillis());

        Truth.assertThat(findAllDocumentEvents()).hasSize(10);
        Truth.assertThat(findAllNotifications()).hasSize(10);
    }

    @Test(timeout = 20000)
    public void shouldStopDeletingDocumentEventsIfLostLock() throws Exception {
        lockStrategy.releaseAll();

        Instant tooOld = fixedClock.instant()
                .minus(DELETE_OLDER_THAN)
                .minus(1, ChronoUnit.SECONDS);

        insertDocumentEventsCreatedAt(tooOld, 10);

        Awaitility.await("Initial delete once route acquires lock")
                .until(this::findAllDocumentEvents, Matchers.hasSize(0));

        // Now let's steal lock...
        lockStrategy.forceAcquire(docEventsDeleterRoute.deleterLockResourceId);

        // Make sure any delete in progress finishes by waiting for the next attempt to acquire lock
        // which should fail.
        lockStrategy.waitForTryAcquire();

        insertDocumentEventsCreatedAt(tooOld, 10);

        Thread.sleep(DELETE_INTERVAL.multipliedBy(3).toMillis());

        Truth.assertThat(findAllDocumentEvents()).hasSize(10);
    }

    @Test(timeout = 20000)
    public void shouldStopDeletingNotificationsIfLostLock() throws Exception {
        lockStrategy.releaseAll();

        Instant tooOld = fixedClock.instant()
                .minus(DELETE_OLDER_THAN)
                .minus(1, ChronoUnit.SECONDS);

        insertNotificationsForRequestsAt(tooOld, 10);

        Awaitility.await("Initial delete once route acquires lock")
                .until(this::findAllNotifications, Matchers.hasSize(0));

        // Now let's steal lock...
        lockStrategy.forceAcquire(notificationsDeleterRoute.deleterLockResourceId);

        // Make sure any delete in progress finishes by waiting for the next attempt to acquire lock
        // which should fail.
        lockStrategy.waitForTryAcquire();

        insertNotificationsForRequestsAt(tooOld, 10);

        Thread.sleep(DELETE_INTERVAL.multipliedBy(3).toMillis());

        Truth.assertThat(findAllNotifications()).hasSize(10);
    }

    @Test
    public void shouldReleaseLocksIfRoutesStop() throws Exception {
        lockStrategy.releaseAll();

        // Wait for locks to be acquired...
        Awaitility.await("Locks are acquired")
                .until(() -> lockStrategy.getAcquired().entrySet(), Matchers.hasSize(2));

        context.stop();

        // Wait for locks to be released...
        Awaitility.await("Locks are released")
                .atMost(DELETE_INTERVAL.multipliedBy(3).getSeconds(), TimeUnit.SECONDS)
                .until(() -> lockStrategy.getAcquired().entrySet(), Matchers.hasSize(0));
    }

    List<DocumentEventEntity> findAllDocumentEvents() throws LightblueException {
        DataFindRequest find = new DataFindRequest(
                DocumentEventEntity.ENTITY_NAME,
                DocumentEventEntity.VERSION);

        find.select(Projection.includeFieldRecursively("*"));

        DocumentEventEntity[] found = client.data(find, DocumentEventEntity[].class);

        return Arrays.asList(found);
    }

    List<NotificationEntity> findAllNotifications() throws LightblueException {
        DataFindRequest find = new DataFindRequest(
                NotificationEntity.ENTITY_NAME,
                NotificationEntity.ENTITY_VERSION);

        find.select(Projection.includeFieldRecursively("*"));

        NotificationEntity[] found = client.data(find, NotificationEntity[].class);

        return Arrays.asList(found);
    }

    void insertDocumentEventsCreatedAt(Instant creationDate, int count)
            throws LightblueException {
        for (int i = 0; i < count; i++) {
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

    void insertNotificationsForRequestsAt(Instant clientRequestDate, int count)
            throws LightblueException {
        for (int i = 0; i < count; i++) {
            StringNotification event = new StringNotification(UUID.randomUUID().toString(),
                    NotificationEntity.Operation.insert, null,
                    Clock.fixed(clientRequestDate, fixedClock.getZone()));

            NotificationEntity entity = event.wrappedNotificationEntity();

            DataInsertRequest insert = new DataInsertRequest(
                    NotificationEntity.ENTITY_NAME,
                    NotificationEntity.ENTITY_VERSION);
            insert.create(entity);

            client.data(insert);
        }
    }
}
