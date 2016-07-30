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

import com.google.common.collect.TreeTraverser;
import com.google.common.truth.Truth;
import com.google.common.util.concurrent.Futures;
import org.apache.camel.EndpointInject;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.ExpressionBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RunWith(JUnit4.class)
public class RetryingBatchFailedMessageRouteTest extends CamelTestSupport {
    @EndpointInject(uri = "direct:failure_batches")
    ProducerTemplate toFailureRetry5Retries;

    @EndpointInject(uri = "direct:failure_batches_long_delay")
    ProducerTemplate toFailureRetry2SecondDelay5Retries;

    @EndpointInject(uri = "mock:direct:dlq")
    MockEndpoint toDlq;

    @Override
    protected RoutesBuilder[] createRouteBuilders() throws Exception {
        return new RoutesBuilder[] {
                new RetryingBatchFailedMessageRoute("direct:failure_batches",
                        /*retryDelay:*/ ExpressionBuilder.constantExpression(10),
                        /*maxRetryCount:*/ 5, /*processTimeout:*/ Duration.ofSeconds(5),
                        "mock:direct:dlq"),
                new RetryingBatchFailedMessageRoute("direct:failure_batches_long_delay",
                        /*retryDelay:*/ ExpressionBuilder.constantExpression(2000),
                        /*maxRetryCount:*/ 5, /*processTimeout:*/ Duration.ofSeconds(5),
                        "mock:direct:dlq"),
        };
    }

    @Test
    public void shouldRetryBatchUpToMaxRetryCount() throws Exception {
        TestRetryMessage alwaysFails1 = TestRetryMessage.neverRecovering();
        FailedMessage failure1 =
                new FailedMessage("original", alwaysFails1, new Exception("Simulated original failure"));

        TestRetryMessage alwaysFails2 = TestRetryMessage.neverRecovering();
        FailedMessage failure2 =
                new FailedMessage("original", alwaysFails2, new Exception("Simulated original failure"));

        toFailureRetry5Retries.sendBody(Arrays.asList(failure1, failure2));

        Truth.assertThat(alwaysFails1.processCount).named("times failure retried").isEqualTo(5);
        Truth.assertThat(alwaysFails2.processCount).named("times failure retried").isEqualTo(5);
    }

    @Test
    public void shouldNotRetryIfNoMoreFailures() throws Exception {
        TestRetryMessage recoversOn3rdTry1 = TestRetryMessage.recoveringAfter(3);
        FailedMessage failure1 = new FailedMessage(
                "original", recoversOn3rdTry1, new Exception("Simulated original failure"));

        TestRetryMessage recoversOn3rdTry2 = TestRetryMessage.recoveringAfter(3);
        FailedMessage failure2 = new FailedMessage(
                "original", recoversOn3rdTry2, new Exception("Simulated original failure"));

        toFailureRetry5Retries.sendBody(Arrays.asList(failure1, failure2));

        Truth.assertThat(recoversOn3rdTry1.processCount).named("times failure retried").isEqualTo(3);
        Truth.assertThat(recoversOn3rdTry2.processCount).named("times failure retried").isEqualTo(3);
    }

    @Test
    public void shouldNotSendAnythingToDlqIfNoMoreFailures() throws Exception {
        TestRetryMessage recoversOn3rdTry1 = TestRetryMessage.recoveringAfter(3);
        FailedMessage failure1 = new FailedMessage(
                "original", recoversOn3rdTry1, new Exception("Simulated original failure"));

        TestRetryMessage recoversOn3rdTry2 = TestRetryMessage.recoveringAfter(3);
        FailedMessage failure2 = new FailedMessage(
                "original", recoversOn3rdTry2, new Exception("Simulated original failure"));

        toDlq.expectedMessageCount(0);

        toFailureRetry5Retries.sendBody(Arrays.asList(failure1, failure2));

        toDlq.assertIsSatisfied();
    }

    @Test
    public void shouldRetryRemainingFailuresAndNotRetryFailuresWhichRecovered() throws Exception {
        TestRetryMessage alwaysFailsMsg = TestRetryMessage.neverRecovering();
        FailedMessage alwaysFails = new FailedMessage(
                "original", alwaysFailsMsg, new Exception("Simulated original failure"));

        TestRetryMessage recoversOn3rdTryMsg = TestRetryMessage.recoveringAfter(3);
        FailedMessage recoversOn3rdTry = new FailedMessage(
                "original", recoversOn3rdTryMsg, new Exception("Simulated original failure"));

        toFailureRetry5Retries.sendBody(Arrays.asList(alwaysFails, recoversOn3rdTry));

        Truth.assertThat(alwaysFailsMsg.processCount).named("times failure retried").isEqualTo(5);
        Truth.assertThat(recoversOn3rdTryMsg.processCount).named("times failure retried").isEqualTo(3);
    }

    @Test
    public void shouldSendRemainingFailuresToDlqOnceRetryCountMaxMetRetainingOriginalMessagesAndAllExceptions()
            throws Exception {
        TestRetryMessage alwaysFailsMsg1 = TestRetryMessage.neverRecovering();
        FailedMessage alwaysFails1 = new FailedMessage(
                "fail original 1", alwaysFailsMsg1, new Exception("Simulated original failure 1"));

        TestRetryMessage alwaysFailsMsg2 = TestRetryMessage.neverRecovering();
        FailedMessage alwaysFails2 = new FailedMessage(
                "fail original 2", alwaysFailsMsg2, new Exception("Simulated original failure 2"));

        TestRetryMessage recoversOn3rdTryMsg = TestRetryMessage.recoveringAfter(3);
        FailedMessage recoversOn3rdTry = new FailedMessage(
                "original", recoversOn3rdTryMsg, new Exception("Simulated original failure"));

        toDlq.expectedMessageCount(1);

        toFailureRetry5Retries.sendBody(Arrays.asList(alwaysFails1, alwaysFails2, recoversOn3rdTry));

        toDlq.assertIsSatisfied();

        Collection<FailedMessage> deadLetters =
                toDlq.getExchanges().get(0).getIn().getMandatoryBody(Collection.class);

        FailedMessage dead1 = deadLetters.stream()
                .filter(m -> m.originalMessage().equals("fail original 1"))
                .findFirst().orElseThrow(AssertionError::new);

        FailedMessage dead2 = deadLetters.stream()
                .filter(m -> m.originalMessage().equals("fail original 2"))
                .findFirst().orElseThrow(AssertionError::new);

        assertEquals(alwaysFailsMsg1, dead1.parsedMessage().get());
        assertEquals(exceptionMessageForRetryAttempt(5), dead1.exception().getMessage());

        SuppressedExceptionTraverser suppressed = new SuppressedExceptionTraverser();

        Truth.assertThat(suppressed.breadthFirstTraversal(dead1.exception())
                .transform(Throwable::getMessage)
                .toList())
                .containsExactly(
                        exceptionMessageForRetryAttempt(5),
                        exceptionMessageForRetryAttempt(4),
                        exceptionMessageForRetryAttempt(3),
                        exceptionMessageForRetryAttempt(2),
                        exceptionMessageForRetryAttempt(1),
                        "Simulated original failure 1")
                .inOrder();

        assertEquals(alwaysFailsMsg2, dead2.parsedMessage().get());
        assertEquals(exceptionMessageForRetryAttempt(5), dead2.exception().getMessage());
        Truth.assertThat(suppressed.breadthFirstTraversal(dead2.exception())
                .transform(Throwable::getMessage)
                .toList())
                .containsExactly(
                        exceptionMessageForRetryAttempt(5),
                        exceptionMessageForRetryAttempt(4),
                        exceptionMessageForRetryAttempt(3),
                        exceptionMessageForRetryAttempt(2),
                        exceptionMessageForRetryAttempt(1),
                        "Simulated original failure 2")
                .inOrder();
    }

    @Test(expected = TimeoutException.class)
    public void shouldDelayBeforeRetrying() throws Exception {
        TestRetryMessage alwaysFailsMsg = TestRetryMessage.neverRecovering();
        FailedMessage alwaysFails = new FailedMessage(
                "fail original", alwaysFailsMsg, new Exception("Simulated original failure"));

        Future<?> retryDoneFuture = toFailureRetry2SecondDelay5Retries.asyncSendBody(
                toFailureRetry2SecondDelay5Retries.getDefaultEndpoint(),
                Collections.singleton(alwaysFails));

        retryDoneFuture.get(6, TimeUnit.SECONDS);
    }

    @Test
    public void shouldNotSuppressPreviousFailureExceptionIfItWouldBeRedundant() throws Exception {
        AlwaysFailsForSameReason alwaysFailsMsg = new AlwaysFailsForSameReason();
        FailedMessage alwaysFails = new FailedMessage("fail original", alwaysFailsMsg,
                new Exception("Simulated original failure"));

        toFailureRetry5Retries.sendBody(Collections.singletonList(alwaysFails));

        Collection<FailedMessage> deadLetters =
                toDlq.getExchanges().get(0).getIn().getMandatoryBody(Collection.class);

        SuppressedExceptionTraverser suppressed = new SuppressedExceptionTraverser();

        Truth.assertThat(suppressed.breadthFirstTraversal(deadLetters.iterator().next().exception())
                .transform(Throwable::getMessage)
                .toList())
                .containsExactly(
                        "Simulated retry failure",
                        "Simulated original failure")
                .inOrder();
    }

    static String exceptionMessageForRetryAttempt(int processCount) {
        return "Simulated retry failure " + processCount;
    }

    static class TestRetryMessage implements Message {
        final int recoverAfter;

        int processCount = 0;

        private TestRetryMessage(int recoverAfter) {
            this.recoverAfter = recoverAfter;
        }

        static TestRetryMessage neverRecovering() {
            return new TestRetryMessage(-1);
        }

        static TestRetryMessage recoveringAfter(int recoverAfter) {
            return new TestRetryMessage(recoverAfter);
        }

        @Override
        public Future<Void> process() {
            processCount++;

            if (processCount == recoverAfter) {
                return Futures.immediateFuture(null);
            }

            return Futures.immediateFailedFuture(
                    new Exception(exceptionMessageForRetryAttempt(processCount)));
        }
    }

    static class AlwaysFailsForSameReason implements Message {

        @Override
        public Future<Void> process() {
            return Futures.immediateFailedFuture(new Exception("Simulated retry failure"));
        }
    }

    static class SuppressedExceptionTraverser extends TreeTraverser<Throwable> {

        @Override
        public Iterable<Throwable> children(Throwable root) {
            return Arrays.asList(root.getSuppressed());
        }
    }
}
