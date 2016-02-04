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

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.integration.test.LightblueExternalResource;
import com.redhat.lightblue.client.request.data.DataInsertRequest;

import com.jayway.awaitility.Awaitility;
import org.apache.camel.EndpointInject;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.apache.camel.util.ObjectHelper;
import org.esbtools.eventhandler.PollingDocumentEventProcessorRoute;
import org.esbtools.eventhandler.PollingNotificationProcessorRoute;
import org.esbtools.eventhandler.lightblue.model.DocumentEventEntity;
import org.esbtools.eventhandler.lightblue.testing.LightblueClientConfigurations;
import org.esbtools.eventhandler.lightblue.testing.LightblueClients;
import org.esbtools.eventhandler.lightblue.testing.MultiStringDocumentEvent;
import org.esbtools.eventhandler.lightblue.testing.MultiStringNotification;
import org.esbtools.eventhandler.lightblue.testing.StringDocumentEvent;
import org.esbtools.eventhandler.lightblue.testing.StringNotification;
import org.esbtools.eventhandler.lightblue.testing.TestMetadataJson;
import org.esbtools.lightbluenotificationhook.NotificationEntity;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class EventHandlerIntegrationTest extends CamelTestSupport {
    @ClassRule
    public static LightblueExternalResource lightblueExternalResource = new LightblueExternalResource(
                    TestMetadataJson.forEntities(NotificationEntity.class, DocumentEventEntity.class));

    // Use two configurations to test concurrency.
    LightblueNotificationRepository notificationRepository1;

    LightblueNotificationRepository notificationRepository2;

    LightblueDocumentEventRepository documentEventRepository1;

    LightblueDocumentEventRepository documentEventRepository2;

    LightblueClient client;

    @EndpointInject(uri = "mock:documents")
    MockEndpoint documentsEndpoint;

    @EndpointInject(uri = "mock:failures")
    MockEndpoint failuresEndpoint;

    static Clock systemUtc = Clock.systemUTC();

    Map<String, DocumentEventFactory> documentEventFactoriesByType =
            new HashMap<String, DocumentEventFactory>() {{
                put("String", StringDocumentEvent::new);
                put("MultiString", MultiStringDocumentEvent::new);
            }};

    Map<String, NotificationFactory> notificationFactoryByEntityName =
            new HashMap<String, NotificationFactory>() {{
                put("String", StringNotification::new);
                put("MultiString", MultiStringNotification::new);
            }};

    @Override
    protected void doPreSetup() throws Exception {
        client = LightblueClients.withJavaTimeSerializationSupport(
                LightblueClientConfigurations.fromLightblueExternalResource(lightblueExternalResource));

        LightblueAutoPingLockStrategy lockStrategy1 = new LightblueAutoPingLockStrategy(
                client.getLocking("testLockingDomain"), Duration.ofSeconds(5));

        LightblueAutoPingLockStrategy lockStrategy2 = new LightblueAutoPingLockStrategy(
                client.getLocking("testLockingDomain"), Duration.ofSeconds(5));

        LightblueNotificationRepositoryConfig notificationConfigRepo1 =
                new MutableLightblueNotificationRepositoryConfig(Arrays.asList("String", "MultiString"),
                        /* processingTimeout */ Duration.ofSeconds(60),
                        /* expireThreshold */ Duration.ofSeconds(30));
        LightblueNotificationRepositoryConfig notificationConfigRepo2 =
                new MutableLightblueNotificationRepositoryConfig(Arrays.asList("String"),
                        /* processingTimeout */ Duration.ofSeconds(60),
                        /* expireThreshold */ Duration.ofSeconds(30));

        notificationRepository1 = new LightblueNotificationRepository(client, lockStrategy1,
                notificationConfigRepo1, notificationFactoryByEntityName, systemUtc);
        notificationRepository2 = new LightblueNotificationRepository(client, lockStrategy2,
                notificationConfigRepo2, notificationFactoryByEntityName, systemUtc);

        LightblueDocumentEventRepositoryConfig configForRepo1 =
                new MutableLightblueDocumentEventRepositoryConfig(Arrays.asList("String", "MultiString"), 100);
        LightblueDocumentEventRepositoryConfig configForRepo2 =
                new MutableLightblueDocumentEventRepositoryConfig(Arrays.asList("String"), 100);

        documentEventRepository1 = new LightblueDocumentEventRepository(client,
                lockStrategy1, configForRepo1, documentEventFactoriesByType, systemUtc);
        documentEventRepository2 = new LightblueDocumentEventRepository(client,
                lockStrategy2, configForRepo2, documentEventFactoriesByType, systemUtc);
    }

    @Override
    protected RouteBuilder[] createRouteBuilders() throws Exception {
        return new RouteBuilder[] {
                new PollingNotificationProcessorRoute(notificationRepository1, documentEventRepository1,
                        Duration.ofSeconds(1), 50),
                new PollingNotificationProcessorRoute(notificationRepository2, documentEventRepository2,
                        Duration.ofSeconds(1), 35),
                new PollingDocumentEventProcessorRoute(documentEventRepository1, Duration.ofSeconds(1),
                        20, "mock:documents", "mock:failures"),
                new PollingDocumentEventProcessorRoute(documentEventRepository2, Duration.ofSeconds(1),
                        10, "mock:documents", "mock:failures")
        };
    }

    @Test
    public void shouldTurnNotificationsIntoDocuments() throws LightblueException {
        NotificationEntity[] stringEntities = randomStringNotificationEntities(200);
        NotificationEntity[] multiStringEntities = randomMultiStringNotificationEntities(0);

        List<String> expectedValues = new ArrayList<>(200);
        Arrays.stream(stringEntities)
                .map(e -> e.getEntityDataForField("value")).forEach(expectedValues::add);
        Arrays.stream(multiStringEntities)
                .flatMap(e -> Arrays.stream(e.getEntityDataForField("values").split("\\|")))
                .forEach(expectedValues::add);

        insertNotificationEntities(stringEntities);
        insertNotificationEntities(multiStringEntities);

        Awaitility.await().atMost(30, TimeUnit.SECONDS)
                .pollDelay(1, TimeUnit.SECONDS)
                .until(() -> documentsEndpoint.getExchanges()
                        .stream()
                        .flatMap(exchange -> StreamSupport.stream(
                                ObjectHelper.createIterable(exchange.getIn().getBody())
                                .spliterator(), false)
                                .map(b -> (String) b))
                        .collect(Collectors.toList()),
                Matchers.containsInAnyOrder(expectedValues.toArray(new String[200])));
    }

    private void insertNotificationEntities(NotificationEntity... entities) throws LightblueException {
        if (entities.length == 0) return;
        DataInsertRequest insertEntities = new DataInsertRequest(
                NotificationEntity.ENTITY_NAME, NotificationEntity.ENTITY_VERSION);
        insertEntities.create(entities);
        client.data(insertEntities);
    }

    private static LightblueNotification notificationForStringInsert(String value) {
        return new StringNotification(value, NotificationEntity.Operation.INSERT, "tester", systemUtc);
    }

    private static NotificationEntity notificationEntityForStringInsert(String value) {
        return notificationForStringInsert(value).wrappedNotificationEntity();
    }

    private static LightblueNotification notificationForMultiStringInsert(List<String> values) {
        return new MultiStringNotification(values, NotificationEntity.Operation.INSERT, "tester",
                systemUtc);
    }

    private static NotificationEntity notificationEntityForMultiStringInsert(List<String> values) {
        return notificationForMultiStringInsert(values).wrappedNotificationEntity();
    }

    private static NotificationEntity[] randomStringNotificationEntities(int amount) {
        NotificationEntity[] entities = new NotificationEntity[amount];

        for (int i = 0; i < amount; i++) {
            entities[i] = notificationEntityForStringInsert(Integer.toString(i));
        }

        return entities;
    }

    private static NotificationEntity[] randomMultiStringNotificationEntities(int amount) {
        NotificationEntity[] entities = new NotificationEntity[amount];

        for (int i = 0; i < amount; i++) {
            entities[i] = notificationEntityForMultiStringInsert(Arrays.asList(Integer.toString(i * -1)));
        }

        return entities;
    }
}
