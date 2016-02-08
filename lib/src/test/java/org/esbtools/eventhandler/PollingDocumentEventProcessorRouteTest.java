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

import com.google.common.truth.Truth;
import com.jayway.awaitility.Awaitility;
import org.apache.camel.EndpointInject;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.esbtools.eventhandler.testing.FailingDocumentEvent;
import org.esbtools.eventhandler.testing.SimpleInMemoryDocumentEventRepository;
import org.esbtools.eventhandler.testing.StringDocumentEvent;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class PollingDocumentEventProcessorRouteTest extends CamelTestSupport {
    SimpleInMemoryDocumentEventRepository documentEventRepository = new SimpleInMemoryDocumentEventRepository();

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

        documentEventRepository.addNewDocumentEvents(randomSuccessfulEvents(10));
        Thread.sleep(5000);
        documentEventRepository.addNewDocumentEvents(randomSuccessfulEvents(10));
        Thread.sleep(5000);
        documentEventRepository.addNewDocumentEvents(randomSuccessfulEvents(10));

        documentEndpoint.assertIsSatisfied();
    }

    @Test
    public void shouldSendFailedEventsToTheFailureEndpointButProcessRest() throws Exception {
        failureEndpoint.expectedMessageCount(4);
        documentEndpoint.expectedMessageCount(6);

        List<DocumentEvent> events = new ArrayList<>(10);
        events.addAll(randomFailingEvents(4));
        events.addAll(randomSuccessfulEvents(6));

        documentEventRepository.addNewDocumentEvents(events);

        documentEndpoint.assertIsSatisfied();
        failureEndpoint.assertIsSatisfied();
    }

    @Test
    public void shouldMarkFailedEventsAsFailed() throws Exception {
        List<DocumentEvent> events = new ArrayList<>(10);
        events.addAll(randomFailingEvents(4));

        documentEventRepository.addNewDocumentEvents(events);

        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(documentEventRepository::getFailedEvents, Matchers.hasSize(4));
    }

    @Test
    public void shouldDropEventsWhoseTransactionsAreNoLongerActive() throws Exception {
        documentEndpoint.expectedMessageCount(0);
        failureEndpoint.expectedMessageCount(0);

        documentEventRepository.considerNoTransactionsActive();
        documentEventRepository.addNewDocumentEvents(randomSuccessfulEvents(10));

        documentEndpoint.assertIsSatisfied(5000);
        failureEndpoint.assertIsSatisfied(100);
    }

    @Test
    public void shouldMarkEventsAsPublishedAfterPublishingSuccessfully() throws Exception {
        documentEndpoint.expectedMessageCount(5);

        List<StringDocumentEvent> events = randomSuccessfulEvents(5);

        documentEventRepository.addNewDocumentEvents(events);

        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(documentEventRepository::getPublishedEvents, Matchers.hasSize(5));
    }

    @Test(timeout = 10000)
    // Eventually this should be "shouldRollBackEventsWherePublishFailed"
    // See: https://github.com/esbtools/event-handler/issues/18
    public void shouldNotUpdateEventsAsPublishedOrFailedIfPublishFailed() throws Exception {
        int eventCount = 5;
        CountDownLatch latch = new CountDownLatch(eventCount);

        documentEndpoint.whenAnyExchangeReceived(exchange -> {
            latch.countDown();
            throw new Exception("Simulated publish failure");
        });

        documentEventRepository.addNewDocumentEvents(randomSuccessfulEvents(eventCount));

        latch.await();

        Truth.assertThat(documentEventRepository.getPublishedEvents()).isEmpty();
    }

    public static List<StringDocumentEvent> randomSuccessfulEvents(int amount) {
        List<StringDocumentEvent> events = new ArrayList<>(amount);
        for (int i = 0; i < amount; i++) {
            events.add(new StringDocumentEvent(UUID.randomUUID().toString()));
        }
        return events;
    }

    public static List<FailingDocumentEvent> randomFailingEvents(int amount) {
        List<FailingDocumentEvent> events = new ArrayList<>(amount);
        for (int i = 0; i < amount; i++) {
            events.add(new FailingDocumentEvent());
        }
        return events;
    }
}
