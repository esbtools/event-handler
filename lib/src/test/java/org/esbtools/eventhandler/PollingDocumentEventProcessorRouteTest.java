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

package org.esbtools.eventhandler;

import org.apache.camel.EndpointInject;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.esbtools.eventhandler.testing.SimpleInMemoryDocumentEventRepository;
import org.esbtools.eventhandler.testing.StringDocumentEvent;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class PollingDocumentEventProcessorRouteTest extends CamelTestSupport {
    DocumentEventRepository documentEventRepository = new SimpleInMemoryDocumentEventRepository();

    @EndpointInject(uri = "mock:documents")
    MockEndpoint documentEndpoint;

    @EndpointInject(uri = "mock:failures")
    MockEndpoint failureEndpoint;

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new PollingDocumentEventProcessorRoute(documentEventRepository, Duration.ofSeconds(1),
                10, "mock:documents", "mock:failures");
    }

    @Test
    public void shouldTurnDocumentEventsIntoDocumentsInPeriodicIntervals() throws Exception {
        documentEndpoint.expectedMessageCount(30);

        documentEventRepository.addNewDocumentEvents(randomEvents(10));
        Thread.sleep(5000);
        documentEventRepository.addNewDocumentEvents(randomEvents(10));
        Thread.sleep(5000);
        documentEventRepository.addNewDocumentEvents(randomEvents(10));

        documentEndpoint.assertIsSatisfied();
    }

    public static List<StringDocumentEvent> randomEvents(int amount) {
        List<StringDocumentEvent> events = new ArrayList<>(amount);
        for (int i = 0; i < amount; i++) {
            events.add(new StringDocumentEvent(UUID.randomUUID().toString()));
        }
        return events;
    }
}
