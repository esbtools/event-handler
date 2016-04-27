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

package org.esbtools.eventhandler.lightblue.config;

import org.esbtools.eventhandler.lightblue.testing.LightblueClientConfigurations;
import org.esbtools.eventhandler.lightblue.testing.LightblueClients;
import org.esbtools.eventhandler.lightblue.testing.TestMetadataJson;

import com.jayway.awaitility.Awaitility;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueClientConfiguration;
import com.redhat.lightblue.client.integration.test.LightblueExternalResource;
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
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class PollingLightblueConfigUpdateRouteTest extends CamelTestSupport {
    @ClassRule
    public static LightblueExternalResource lightblueExternalResource =
            new LightblueExternalResource(TestMetadataJson.forEntity(EventHandlerConfigEntity.class));

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    LightblueClient client;
    MutableLightblueNotificationRepositoryConfig notificationRepositoryConfig =
            new MutableLightblueNotificationRepositoryConfig();
    MutableLightblueDocumentEventRepositoryConfig documentEventRepositoryConfig =
            new MutableLightblueDocumentEventRepositoryConfig();

    static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);

    @Override
    public void doPreSetup() {
        LightblueClientConfiguration lbClientConfig = LightblueClientConfigurations
                .fromLightblueExternalResource(lightblueExternalResource);
        client = LightblueClients.withJavaTimeSerializationSupport(lbClientConfig);
    }

    @Before
    public void dropConfigEntities() throws UnknownHostException {
        lightblueExternalResource.cleanupMongoCollections(EventHandlerConfigEntity.ENTITY_NAME);
    }

    @Override
    protected RoutesBuilder createRouteBuilder() throws Exception {
        return new PollingLightblueConfigUpdateRoute("testDomain", Duration.ofSeconds(1), client,
                notificationRepositoryConfig,
                documentEventRepositoryConfig);
    }

    @Test
    public void shouldUpdateMaxDocumentEventsPerInsert() throws Exception {
        DataInsertRequest insertConfig = new DataInsertRequest(EventHandlerConfigEntity.ENTITY_NAME,
                EventHandlerConfigEntity.ENTITY_VERSION);

        EventHandlerConfigEntity configEntity = new EventHandlerConfigEntity();
        configEntity.setDomain("testDomain");
        configEntity.setMaxDocumentEventsPerInsert(10);

        insertConfig.create(configEntity);

        client.data(insertConfig);

        Awaitility.await()
                .atMost(TEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .until(() -> documentEventRepositoryConfig.getOptionalMaxDocumentEventsPerInsert(),
                        Matchers.equalTo(Optional.of(10)));
    }
}
