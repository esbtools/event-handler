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
import com.google.common.util.concurrent.Futures;
import com.jayway.awaitility.Awaitility;
import org.apache.camel.EndpointInject;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@RunWith(JUnit4.class)
public class AsyncMessageProcessorRouteTest extends CamelTestSupport {
    List<Object> persistence = new ArrayList<>();

    MessageFactory messageFactory = new ByTypeMessageFactory(persistence);

    @EndpointInject(uri = "direct:incoming")
    ProducerTemplate toIncoming;

    @EndpointInject(uri = "direct:short_timeout")
    ProducerTemplate toShortTimeout;

    @EndpointInject(uri = "mock:failures")
    MockEndpoint toFailures;

    /**
     * Demonstrates the kind of message parsing behavior a real factory might do by creating one of
     * three types of messages based on the provided object:
     *
     * <ul>
     *     <li>{@link FailingMessage} when the object is an Exception</li>
     *     <li>{@link TimeConsumingMessage} when the object is a {@link Duration}</li>
     *     <li>In all other cases, creates a {@link TracingPersistingMessage} which persists when
     *     message processing starts and ends with the provided object as the final result.</li>
     * </ul>
     *
     * <p>Additionally, if the object is a {@link SimulatedMessageFactoryFailure} then this, as you
     * might guess, causes the message factory to throw an exception instead of returning a message.
     */
    static class ByTypeMessageFactory implements MessageFactory {
        private final List<Object> persistence;

        ByTypeMessageFactory(List<Object> persistence) {
            this.persistence = persistence;
        }

        @Override
        public Message getMessageForBody(Object body) {
            if (body instanceof Exception) {
                return new FailingMessage((Exception) body);
            }

            if (body instanceof Duration) {
                return new TimeConsumingMessage((Duration) body);
            }

            if (body instanceof SimulatedMessageFactoryFailure) {
                throw ((SimulatedMessageFactoryFailure) body).exception;
            }

            return new TracingPersistingMessage(body, persistence);
        }
    }

    /**
     * Creates two message processor routes: one which reads from "direct:incoming" with a long
     * timeout which should not be hit in normal tests, and another with a very short timeout,
     * reading from "direct:short_timeout", in order to test timeout handling.
     */
    @Override
    protected RouteBuilder[] createRouteBuilders() throws Exception {
        return new RouteBuilder[]{
                new AsyncMessageProcessorRoute("direct:incoming", "mock:failures",
                        Duration.ofMinutes(1), messageFactory),
                new AsyncMessageProcessorRoute("direct:short_timeout", "mock:failures",
                        Duration.ofMillis(1), messageFactory)
        };
    }

    @Test(timeout = 1000L)
    public void shouldProcessReceivedMessagesInCollectionInParallel() {
        List<String> messages = Arrays.asList("fun", "with", "messages");

        toIncoming.sendBody(messages);

        Awaitility.await().until(
                () -> persistence,
                Matchers.hasItems("fun", "with", "messages"));

        Truth.assertThat(persistence.subList(0, 3))
                .named("first three persisted results")
                .containsExactly("processing fun", "processing with", "processing messages");
    }

    @Test(expected = Exception.class)
    public void shouldFailIfEndpointDoesNotReceiveACollectionType() {
        toIncoming.sendBody("not a collection");
    }

    @Test
    public void shouldWrapFailuresAndSendToFailureUri() {
        toFailures.expectedMessageCount(1);

        Exception exception = new Exception("Simulated failure");
        List<Exception> messages = Collections.singletonList(exception);

        // Our message factory treats incoming exceptions as failing messages.
        toIncoming.sendBody(messages);

        toFailures.expectedMessageCount(1);

        Object failure = toFailures.getExchanges().get(0).getIn().getBody();

        Truth.assertThat(failure).named("message sent to failure endpoint")
                .isInstanceOf(FailedMessage.class);

        FailedMessage failedMessage = (FailedMessage) failure;

        Truth.assertThat(failedMessage.parsedMessage().get())
                .isEqualTo(new FailingMessage(exception));
        Truth.assertThat(failedMessage.originalMessage()).isEqualTo(exception);
        Truth.assertThat(failedMessage.exception()).isEqualTo(exception);
    }

    @Test
    public void shouldCatchMessageFactoryFailures() throws InterruptedException {
        toFailures.expectedMessageCount(1);

        RuntimeException exception = new RuntimeException("Simulated runtime exception while parsing message");
        SimulatedMessageFactoryFailure originalMsg = new SimulatedMessageFactoryFailure(exception);

        toIncoming.sendBody(Collections.singleton(originalMsg));

        toFailures.assertIsSatisfied();

        Object failure = toFailures.getExchanges().get(0).getIn().getBody();

        Truth.assertThat(failure).named("message sent to failure endpoint")
                .isInstanceOf(FailedMessage.class);

        FailedMessage failedMessage = (FailedMessage) failure;

        Truth.assertThat(failedMessage.parsedMessage().isPresent())
                .named("presence of a parsed message").isFalse();
        Truth.assertThat(failedMessage.originalMessage()).isEqualTo(originalMsg);
        Truth.assertThat(failedMessage.exception()).isEqualTo(exception);
    }

    @Test(timeout = 1000L)
    public void shouldKeepProcessingRemainingMessagesDespiteFailures() {
        List<Object> messages = Arrays.asList(
                new SimulatedMessageFactoryFailure(new RuntimeException("Simulated parse failure")),
                new Exception("Simulated processing failure"),
                "success!");

        toIncoming.sendBody(messages);

        Awaitility.await().until(
                () -> persistence,
                Matchers.hasItem("success!"));
    }

    @Test(timeout = 1000L)
    public void shouldWrapTimeoutsInRecoverableExceptionsAndSendToFailuresUri() throws InterruptedException {
        toFailures.expectedMessageCount(1);

        toShortTimeout.sendBody(Arrays.asList(Duration.ofSeconds(5)));

        toFailures.assertIsSatisfied();

        Object failure = toFailures.getExchanges().get(0).getIn().getBody();

        Truth.assertThat(failure).named("message sent to failure endpoint")
                .isInstanceOf(FailedMessage.class);

        FailedMessage failedMessage = (FailedMessage) failure;

        Truth.assertThat(failedMessage.parsedMessage().get()).isInstanceOf(TimeConsumingMessage.class);
        Truth.assertThat(failedMessage.originalMessage()).isEqualTo(Duration.ofSeconds(5));
        Truth.assertThat(failedMessage.exception()).isInstanceOf(RecoverableException.class);
    }

    static class FailingMessage implements Message {
        private final Exception exception;

        FailingMessage(Exception exception) {
            this.exception = exception;
        }

        @Override
        public Future<?> process() {
            return Futures.immediateFailedFuture(exception);
        }

        @Override
        public String toString() {
            return "FailingMessage{" +
                    "exception=" + exception +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FailingMessage that = (FailingMessage) o;
            return Objects.equals(exception, that.exception);
        }

        @Override
        public int hashCode() {
            return Objects.hash(exception);
        }
    }

    static class TimeConsumingMessage implements Message {
        private final Duration duration;

        TimeConsumingMessage(Duration duration) {
            this.duration = duration;
        }

        @Override
        public Future<?> process() {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                return executor.submit(() -> {
                    try {
                        Thread.sleep(duration.toMillis());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            } finally {
                executor.shutdown();
            }
        }

        @Override
        public String toString() {
            return "TimeConsumingMessage{" +
                    "duration=" + duration +
                    '}';
        }
    }

    /**
     * Tracks when processing starts and when processing is done.
     */
    static class TracingPersistingMessage implements Message {
        private final Object body;
        private final List<Object> persistence;

        TracingPersistingMessage(Object body, List<Object> persistence) {
            this.body = body;
            this.persistence = persistence;
        }

        @Override
        public Future<?> process() {
            persistence.add("processing " + body);

            return Futures.lazyTransform(
                    Futures.immediateFuture(body),
                    (e) -> persistence.add(e));
        }

        @Override
        public String toString() {
            return "LazyPersistingMessage{" +
                    "body=" + body +
                    '}';
        }
    }

    static class SimulatedMessageFactoryFailure {
        private final RuntimeException exception;

        public SimulatedMessageFactoryFailure(RuntimeException exception) {
            this.exception = exception;
        }
    }
}
