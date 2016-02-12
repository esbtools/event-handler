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

import org.esbtools.eventhandler.FailedNotification;
import org.esbtools.eventhandler.lightblue.config.MutableLightblueNotificationRepositoryConfig;
import org.esbtools.eventhandler.lightblue.testing.InMemoryLockStrategy;
import org.esbtools.eventhandler.lightblue.testing.LightblueClientConfigurations;
import org.esbtools.eventhandler.lightblue.testing.LightblueClients;
import org.esbtools.eventhandler.lightblue.testing.MultiStringNotification;
import org.esbtools.eventhandler.lightblue.testing.SlowDataLightblueClient;
import org.esbtools.eventhandler.lightblue.testing.StringNotification;
import org.esbtools.eventhandler.lightblue.testing.TestMetadataJson;
import org.esbtools.lightbluenotificationhook.NotificationEntity;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.annotation.Nullable;
import java.net.UnknownHostException;
import java.sql.Date;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@RunWith(JUnit4.class)
public class LightblueNotificationRepositoryTest {
    @ClassRule
    public static LightblueExternalResource lightblueExternalResource =
            new LightblueExternalResource(TestMetadataJson.forEntity(NotificationEntity.class));

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private LightblueClient client;

    private LightblueNotificationRepository repository;

    private final Map<String, NotificationFactory> notificationFactoryByEntityName =
            new HashMap<String, NotificationFactory>() {{
                put("String", StringNotification::new);
                put("MultiString", MultiStringNotification::new);
            }};

    private final MutableLightblueNotificationRepositoryConfig config =
            new MutableLightblueNotificationRepositoryConfig()
                    .setEntityNamesToProcess(notificationFactoryByEntityName.keySet())
                    .setNotificationProcessingTimeout(PROCESSING_TIMEOUT)
                    .setNotificationExpireThreshold(EXPIRE_THRESHOLD);

    private final InMemoryLockStrategy lockStrategy = new InMemoryLockStrategy();

    private static final Clock fixedClock = Clock.fixed(Instant.now(), ZoneId.of("GMT"));
    private static final Duration PROCESSING_TIMEOUT = Duration.ofMinutes(1);
    private static final Duration EXPIRE_THRESHOLD = Duration.ofSeconds(30);

    @Before
    public void initializeLightblueClientAndRepository() {
        LightblueClientConfiguration lbClientConfig = LightblueClientConfigurations
                .fromLightblueExternalResource(lightblueExternalResource);
        client = LightblueClients.withJavaTimeSerializationSupport(lbClientConfig);

        // TODO: Try and reduce places canonical types are specified
        // We have 3 here: type list to process, types to factories, and inside the doc event impls
        // themselves.
        repository = new LightblueNotificationRepository(client, lockStrategy, config,
                notificationFactoryByEntityName, fixedClock);
    }

    @Before
    public void dropNotificationEntities() throws UnknownHostException {
        lightblueExternalResource.cleanupMongoCollections(NotificationEntity.ENTITY_NAME);
    }

    @Test
    public void shouldRetrieveNotificationsForSpecifiedEntities() throws Exception {
        NotificationEntity notIncludedEntity = new NotificationEntity();
        notIncludedEntity.setEntityName("other");
        notIncludedEntity.setEntityData(Arrays.asList(new NotificationEntity.PathAndValue("foo", null)));
        notIncludedEntity.setEntityVersion("1.0.0");
        notIncludedEntity.setStatus(NotificationEntity.Status.unprocessed);
        notIncludedEntity.setOccurrenceDate(Date.from(fixedClock.instant()));
        notIncludedEntity.setOperation(NotificationEntity.Operation.insert);
        notIncludedEntity.setTriggeredByUser("tester");

        NotificationEntity entity1 = notificationEntityForStringInsert("1", fixedClock.instant());
        entity1.set_id("thisShouldBeTheOnlyId");

        insertNotificationEntities(notIncludedEntity, entity1);

        List<LightblueNotification> retrieved = repository.retrieveOldestNotificationsUpTo(2);

        assertThat(retrieved).hasSize(1);
        assertThat(retrieved.get(0).wrappedNotificationEntity().get_id())
                .isEqualTo("thisShouldBeTheOnlyId");
    }

    @Test
    public void shouldOnlyRetrieveUnprocessedNotifications() throws Exception {
        NotificationEntity unprocessedEntity = notificationEntityForStringInsert("right");
        NotificationEntity processingEntity = notificationEntityForStringInsert("wrong");
        processingEntity.setStatus(NotificationEntity.Status.processing);
        NotificationEntity processedEntity = notificationEntityForStringInsert("wrong");
        processedEntity.setStatus(NotificationEntity.Status.processed);
        NotificationEntity failedEntity = notificationEntityForStringInsert("wrong");
        failedEntity.setStatus(NotificationEntity.Status.failed);

        insertNotificationEntities(unprocessedEntity, processedEntity, processedEntity, failedEntity);

        List<LightblueNotification> retrieved = repository.retrieveOldestNotificationsUpTo(4);

        assertThat(retrieved).hasSize(1);
        assertThat(retrieved.get(0).wrappedNotificationEntity().getEntityDataForField("value"))
                .isEqualTo("right");
    }

    @Test
    public void shouldRetrieveNotificationsOldestFirstUpToRequestedMax() throws Exception {
        NotificationEntity entity1 = notificationEntityForStringInsert("1", fixedClock.instant());
        NotificationEntity entity2 = notificationEntityForStringInsert("2", fixedClock.instant().plus(1, ChronoUnit.MINUTES));
        NotificationEntity entity3 = notificationEntityForStringInsert("3", fixedClock.instant().plus(2, ChronoUnit.MINUTES));
        NotificationEntity entity4 = notificationEntityForStringInsert("4", fixedClock.instant().plus(3, ChronoUnit.MINUTES));

        insertNotificationEntities(entity3, entity1, entity4, entity2);

        List<LightblueNotification> retrieved = repository.retrieveOldestNotificationsUpTo(3);

        assertThat(retrieved.stream()
                .map(notification -> notification
                        .wrappedNotificationEntity()
                        .getEntityDataForField("value"))
                .collect(Collectors.toList()))
                .containsExactly("1", "2", "3");
    }

    @Test
    public void shouldMarkRetrievedNotificationsAsProcessing() throws Exception {
        insertNotificationEntities(randomNotificationEntities(4));

        repository.retrieveOldestNotificationsUpTo(4);

        List<NotificationEntity> allEntities = findNotificationEntitiesWhere(null);

        assertThat(allEntities.stream()
                .map(entity -> entity.getStatus().toString())
                .collect(Collectors.toList()))
                .containsExactly("processing", "processing", "processing", "processing");
    }

    @Test
    public void shouldRetrieveNonOverlappingSetsOfNotificationsIfCalledByMultipleThreads()
            throws LightblueException, InterruptedException, TimeoutException, ExecutionException {
        SlowDataLightblueClient thread1Client = new SlowDataLightblueClient(client);
        SlowDataLightblueClient thread2Client = new SlowDataLightblueClient(client);

        LightblueNotificationRepository thread1Repository = new LightblueNotificationRepository(
                thread1Client, new InMemoryLockStrategy(), config,
                notificationFactoryByEntityName, fixedClock);

        LightblueNotificationRepository thread2Repository = new LightblueNotificationRepository(
                thread1Client, new InMemoryLockStrategy(), config,
                notificationFactoryByEntityName, fixedClock);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            NotificationEntity[] entities = randomNotificationEntities(20);

            List<String> expectedValues = Arrays.stream(entities)
                    .map(e -> e.getEntityDataForField("value"))
                    .collect(Collectors.toList());

            insertNotificationEntities(entities);

            CountDownLatch bothThreadsStarted = new CountDownLatch(2);

            thread1Client.pauseOnNextRequest();
            thread2Client.pauseOnNextRequest();

            Future<List<LightblueNotification>> futureThread1Events = executor.submit(() -> {
                bothThreadsStarted.countDown();
                return thread1Repository.retrieveOldestNotificationsUpTo(20);
            });
            Future<List<LightblueNotification>> futureThread2Events = executor.submit(() -> {
                bothThreadsStarted.countDown();
                return thread2Repository.retrieveOldestNotificationsUpTo(20);
            });

            bothThreadsStarted.await();

            Thread.sleep(5000);

            thread2Client.unpause();
            thread1Client.unpause();

            List<LightblueNotification> thread1Notifications = futureThread1Events.get(5, TimeUnit.SECONDS);
            List<LightblueNotification> thread2Notifications = futureThread2Events.get(5, TimeUnit.SECONDS);

            List<String> retrievedValues = new ArrayList<>();

            retrievedValues.addAll(thread1Notifications.stream()
                    .map(e -> e.wrappedNotificationEntity().getEntityDataForField("value"))
                    .collect(Collectors.toList()));

            retrievedValues.addAll(thread2Notifications.stream()
                    .map(e -> e.wrappedNotificationEntity().getEntityDataForField("value"))
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
    public void shouldLeaveUnretrievedNotificationsAsUnprocessed() throws Exception {
        insertNotificationEntities(randomNotificationEntities(10));

        List<String> retrievedIds = repository.retrieveOldestNotificationsUpTo(5).stream()
                .map(notification -> notification.wrappedNotificationEntity().get_id())
                .collect(Collectors.toList());

        List<String> unprocessedIds =
                findNotificationEntitiesWhere(Query.withValue("status", Query.BinOp.eq, "unprocessed"))
                        .stream().map(NotificationEntity::get_id).collect(Collectors.toList());

        assertThat(unprocessedIds).containsNoneIn(retrievedIds);
        assertThat(unprocessedIds).hasSize(5);
    }

    @Test
    public void shouldMarkNotificationsAsProcessedOrFailed() throws LightblueException {
        LightblueNotification expectedProcessed = notificationForStringInsert("should succeed");
        LightblueNotification expectedFailed = notificationForStringInsert("should fail");

        NotificationEntity expectedProcessedEntity = expectedProcessed.wrappedNotificationEntity();
        NotificationEntity expectedFailedEntity = expectedFailed.wrappedNotificationEntity();

        expectedProcessedEntity.set_id("1");
        expectedFailedEntity.set_id("2");

        expectedProcessedEntity.setStatus(NotificationEntity.Status.processing);
        expectedFailedEntity.setStatus(NotificationEntity.Status.processing);

        insertNotificationEntities(
                expectedProcessedEntity,
                expectedFailedEntity);

        List<LightblueNotification> succeeded = Arrays.asList(expectedProcessed);
        List<FailedNotification> failed = Arrays.asList(
                new FailedNotification(expectedFailed, new RuntimeException("fake")));

        repository.markNotificationsProcessedOrFailed(succeeded, failed);

        List<NotificationEntity> foundProcessed = findNotificationEntitiesWhere(
                Query.withValue("status", Query.BinOp.eq, "processed"));
        List<NotificationEntity> foundFailed = findNotificationEntitiesWhere(
                Query.withValue("status", Query.BinOp.eq, "failed"));

        assertThat(foundProcessed).named("found processed entities").hasSize(1);
        assertThat(foundFailed).named("found failed entities").hasSize(1);
        assertThat(foundProcessed.get(0).getEntityDataForField("value")).isEqualTo("should succeed");
        assertThat(foundFailed.get(0).getEntityDataForField("value")).isEqualTo("should fail");
    }

    @Test
    public void shouldNotReturnOrUpdateNotificationsWhoseLockWasLostBeforeNotificationStatusUpdatesPersisted()
            throws Exception {
        insertNotificationEntities(randomNotificationEntities(20));

        // Sneakily steal away any acquired locks
        lockStrategy.allowLockButImmediateLoseIt();

        List<LightblueNotification> retrieved = repository.retrieveOldestNotificationsUpTo(10);

        assertThat(retrieved).isEmpty();
    }

    @Test
    public void shouldRecognizeUpdatesToProvidedEntityNamesConfiguration() throws Exception {
        insertNotificationEntities(
                notificationEntityForMultiStringInsert("1"),
                notificationEntityForMultiStringInsert("2"),
                notificationEntityForStringInsert("3"));

        config.setEntityNamesToProcess(Arrays.asList("String"));

        List<LightblueNotification> retrieved = repository.retrieveOldestNotificationsUpTo(3);

        assertThat(retrieved).hasSize(1);
        assertThat(retrieved.get(0).wrappedNotificationEntity().getEntityDataForField("value")).isEqualTo("3");
    }

    @Test(expected = Exception.class)
    public void shouldTreatExpiredNotificationsTransactionsAsInactive() throws Exception {
        // Slightly older than the expire threshold within the processing timeout.
        // If we timeout after 60 seconds, but we drop events within 20 seconds of that,
        // then this must be older than 40 seconds.
        Instant expiredDate = fixedClock.instant()
                .minus(PROCESSING_TIMEOUT)
                .plus(EXPIRE_THRESHOLD)
                .minus(Duration.ofMillis(1));

        LightblueNotification notification = notificationThatStartedProcessingAt(expiredDate);

        repository.ensureTransactionActive(notification);
    }

    @Test
    public void shouldTreatNotExpiredNotificationsTransactionsAsActive() throws Exception {
        // Slightly newer than the expire threshold within the processing timeout.
        // If we timeout after 60 seconds, but we drop events within 20 seconds of that,
        // then this must be newer than 40 seconds.
        Instant notExpiredDate = fixedClock.instant()
                .minus(PROCESSING_TIMEOUT)
                .plus(EXPIRE_THRESHOLD)
                .plus(Duration.ofMillis(1));

        LightblueNotification notification = notificationThatStartedProcessingAt(notExpiredDate);

        repository.ensureTransactionActive(notification);
    }

    @Test
    public void shouldRetrieveTimedOutNotificationsEvenThoughTheyAreProcessing() throws Exception {
        // Slightly older than the processing timeout.
        Instant timedout = fixedClock.instant()
                .minus(PROCESSING_TIMEOUT)
                .minus(Duration.ofMillis(1));

        LightblueNotification notification = notificationThatStartedProcessingAt(timedout);

        insertNotificationEntities(notification.wrappedNotificationEntity());

        assertThat(repository.retrieveOldestNotificationsUpTo(1)).hasSize(1);
    }

    @Test
    public void shouldNotRetrieveNotYetTimedOutProcessingNotifications() throws Exception {
        // Slightly newer than the processing timeout.
        Instant timedout = fixedClock.instant()
                .minus(PROCESSING_TIMEOUT)
                .plus(Duration.ofMillis(1));

        LightblueNotification notification = notificationThatStartedProcessingAt(timedout);

        insertNotificationEntities(notification.wrappedNotificationEntity());

        assertThat(repository.retrieveOldestNotificationsUpTo(1)).isEmpty();
    }

    @Test(expected = Exception.class)
    public void shouldRecognizeUpdatesToProvidedTimeoutsConfiguration() throws Exception {
        Duration newProcessingTimeout = PROCESSING_TIMEOUT.dividedBy(2);
        Duration newExpireThreshold = EXPIRE_THRESHOLD.dividedBy(2);

        config.setNotificationProcessingTimeout(newProcessingTimeout);
        config.setNotificationExpireThreshold(newExpireThreshold);

        Instant expiredDate = fixedClock.instant()
                .minus(newProcessingTimeout)
                .plus(newExpireThreshold)
                .minus(Duration.ofMillis(1));

        LightblueNotification notification = notificationThatStartedProcessingAt(expiredDate);

        repository.ensureTransactionActive(notification);
    }

    private List<NotificationEntity> findNotificationEntitiesWhere(@Nullable Query query)
            throws LightblueException {
        DataFindRequest request = new DataFindRequest(
                NotificationEntity.ENTITY_NAME,
                NotificationEntity.ENTITY_VERSION);

        request.where(query);
        request.select(Projection.includeFieldRecursively("*"));

        return Arrays.asList(client.data(request, NotificationEntity[].class));
    }

    private void insertNotificationEntities(NotificationEntity... entities) throws LightblueException {
        DataInsertRequest insertEntities = new DataInsertRequest(
                NotificationEntity.ENTITY_NAME, NotificationEntity.ENTITY_VERSION);
        insertEntities.create(entities);
        client.data(insertEntities);
    }

    private static LightblueNotification notificationForStringInsert(String value) {
        return new StringNotification(
                value, NotificationEntity.Operation.insert, "tester", fixedClock);
    }

    private static LightblueNotification notificationThatStartedProcessingAt(Instant processingDate) {
        LightblueNotification notification = notificationForStringInsert("processing");
        NotificationEntity expiredEntity = notification.wrappedNotificationEntity();
        expiredEntity.setStatus(NotificationEntity.Status.processing);
        expiredEntity.setProcessingDate(Date.from(processingDate));
        return notification;
    }

    private static NotificationEntity notificationEntityForStringInsert(String value) {
        return notificationForStringInsert(value).wrappedNotificationEntity();
    }

    private static NotificationEntity notificationEntityForStringInsert(String value,
            Instant occurrenceDate) {
        return new StringNotification(value, NotificationEntity.Operation.insert, "tester",
                Clock.fixed(occurrenceDate, ZoneOffset.UTC)).wrappedNotificationEntity();
    }

    private static NotificationEntity notificationEntityForMultiStringInsert(String value) {
        return new MultiStringNotification(Arrays.asList(value), NotificationEntity.Operation.insert,
                "tester", fixedClock).wrappedNotificationEntity();
    }

    private static NotificationEntity[] randomNotificationEntities(int amount) {
        NotificationEntity[] entities = new NotificationEntity[amount];

        for (int i = 0; i < amount; i++) {
            entities[i] = notificationEntityForStringInsert(Integer.toString(i));
        }

        return entities;
    }
}
