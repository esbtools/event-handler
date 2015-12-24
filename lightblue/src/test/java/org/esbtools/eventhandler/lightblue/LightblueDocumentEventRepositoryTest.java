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

import static org.junit.Assert.assertEquals;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueClientConfiguration;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.integration.test.LightblueExternalResource;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.DataInsertRequest;
import com.redhat.lightblue.client.response.LightblueDataResponse;
import com.redhat.lightblue.client.response.LightblueException;

import org.esbtools.eventhandler.DocumentEvent;
import org.esbtools.eventhandler.lightblue.model.DocumentEventEntity;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;

public class LightblueDocumentEventRepositoryTest {

    @Rule
    public LightblueExternalResource lightblueExternalResource =
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
                new ByTypeDocumentEventFactory().addType("String", StringDocumentEvent::new));
    }

    @Before
    public void dropDocEventEntities() throws UnknownHostException {
        lightblueExternalResource.cleanupMongoCollections(DocumentEventEntity.ENTITY_NAME);
    }

    @Test
    public void shouldRetrieveDocumentEventsForSpecifiedEntities() throws LightblueException {
        DocumentEventEntity stringEvent = new StringDocumentEvent("foo", fixedClock)
                .toNewDocumentEventEntity();

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
        DocumentEventEntity stringEvent = new StringDocumentEvent("foo", fixedClock)
                .toNewDocumentEventEntity();
        insertDocumentEventEntities(stringEvent);

        repository.retrievePriorityDocumentEventsUpTo(1);

        DataFindRequest findDocEvent = new DataFindRequest(DocumentEventEntity.ENTITY_NAME, DocumentEventEntity.VERSION);
        findDocEvent.select(Projection.includeFieldRecursively("*"));
        findDocEvent.where(Query.withValue("canonicalType", Query.BinOp.eq, "String"));
        DocumentEventEntity found = client.data(findDocEvent).parseProcessed(DocumentEventEntity.class);

        assertEquals(DocumentEventEntity.Status.processing, found.getStatus());
    }

    private void insertDocumentEventEntities(DocumentEventEntity... entities) throws LightblueException {
        DataInsertRequest insertEntities = new DataInsertRequest("documentEvent", DocumentEventEntity.VERSION);
        insertEntities.create(entities);
        client.data(insertEntities);
    }
}
