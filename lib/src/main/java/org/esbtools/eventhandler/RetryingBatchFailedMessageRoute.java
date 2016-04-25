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

import static org.apache.camel.builder.PredicateBuilder.and;
import static org.apache.camel.builder.PredicateBuilder.not;

import org.apache.camel.Exchange;
import org.apache.camel.Expression;
import org.apache.camel.Predicate;
import org.apache.camel.builder.RouteBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class RetryingBatchFailedMessageRoute extends RouteBuilder {
    private final String fromUri;
    private final Expression retryDelayMillis;
    private final int maxRetryCount;
    private final Duration processTimeout;
    private final String deadLetterUri;

    private final int idCount = idCounter.getAndIncrement();
    private final String routeId = "failedMessageRetryer-" + idCount;

    private static final AtomicInteger idCounter = new AtomicInteger(0);

    private static final String NEXT_ATTEMPT_NUMBER_PROPERTY = "nextAttemptNumber";
    private static final Integer FIRST_ATTEMPT_NUMBER = 1;

    public RetryingBatchFailedMessageRoute(String fromUri, Expression retryDelayMillis,
            int maxRetryCount, Duration processTimeout, String deadLetterUri) {
        this.fromUri = fromUri;
        this.retryDelayMillis = retryDelayMillis;
        this.maxRetryCount = maxRetryCount;
        this.processTimeout = processTimeout;
        this.deadLetterUri = deadLetterUri;
    }

    @Override
    public void configure() throws Exception {
        from(fromUri)
        .routeId(routeId)
        // We use loop instead of error handler because error handlers start with original message
        // sent to point of failure; we need the message to stay intact to prevent reprocessing
        // already succeeded messages and to keep context of previous tries' failures.
        .loopDoWhile(and(
                exchangeHasFailures(),
                not(maxRetryCountMet())))
            .delay(retryDelayMillis)
                // Indenting because delay actually starts a child processor, and needs its own end()
                // See: https://issues.apache.org/jira/browse/CAMEL-2654
                .process(exchange -> {
                    Integer retryAttempt = Optional.ofNullable(
                            exchange.getProperty(NEXT_ATTEMPT_NUMBER_PROPERTY, Integer.class))
                            .orElse(FIRST_ATTEMPT_NUMBER);
                    // Preemptively increment retry attempt for next loop.
                    exchange.setProperty(NEXT_ATTEMPT_NUMBER_PROPERTY, retryAttempt + 1);

                    Collection oldFailures = exchange.getIn().getMandatoryBody(Collection.class);

                    List<FailedMessage> newFailures = new ArrayList<>();
                    List<ReprocessingFailure> reprocessingFailures =
                            new ArrayList<>(oldFailures.size());

                    log.debug("About to retry {} messages on route {}, attempt #{}: {}",
                            oldFailures.size(), routeId, retryAttempt, oldFailures);

                    // Begin processing all failed messages again in parallel.
                    for (Object failureAsObject : oldFailures) {
                        if (!(failureAsObject instanceof FailedMessage)) {
                            throw new IllegalArgumentException("Messages sent to " +
                                    RetryingBatchFailedMessageRoute.class + " route should be " +
                                    "collections of FailedMessage elements, but got collection " +
                                    "of " + failureAsObject.getClass());
                        }

                        FailedMessage failure = (FailedMessage) failureAsObject;
                        Optional<Message> maybeMessage = failure.parsedMessage();

                        if (!maybeMessage.isPresent()) {
                            // Nothing to retry; dead letter it
                            // This happens when message factory failed to get message from original
                            // body. We won't bother trying get the message from the message factory
                            // again; if that fails it is usually a bug that retrying won't circumvent.
                            log.warn("Failed message had no parsed message. There is no message " +
                                    "to retry without trying to parse again, which is usually " +
                                    "fruitless. Sending to dead letter URI {}.", deadLetterUri);
                            continue;
                        }

                        Message message = maybeMessage.get();
                        final Future<Void> reprocessingFuture;

                        try {
                            reprocessingFuture = message.process();
                        } catch (Exception e) {
                            log.error("Failed to reprocess message (retry attempt #" +
                                    retryAttempt + "): " + message, e);
                            suppressPreviousFailureInNewException(failure, e);
                            newFailures.add(new FailedMessage(failure.originalMessage(), message, e));
                            continue;
                        }

                        reprocessingFailures.add(new ReprocessingFailure(failure, reprocessingFuture));
                    }

                    List<Message> reprocessedSuccessfully = log.isDebugEnabled()
                            ? new ArrayList<>(reprocessingFailures.size())
                            : Collections.emptyList();

                    for (ReprocessingFailure reprocessingFailure : reprocessingFailures) {
                        FailedMessage originalFailure = reprocessingFailure.originalFailure;

                        try {
                            reprocessingFailure.reprocessingFuture
                                    .get(processTimeout.toMillis(), TimeUnit.MILLISECONDS);

                            if (log.isDebugEnabled()) {
                                reprocessedSuccessfully.add(originalFailure.parsedMessage().get());
                            }
                        } catch (ExecutionException e) {
                            Message parsedMessage = originalFailure.parsedMessage().get();

                            log.error("Failed to reprocess message (retry attempt #" + retryAttempt +
                                    "): " + parsedMessage, e);

                            Throwable realException = e.getCause();
                            suppressPreviousFailureInNewException(originalFailure, realException);

                            FailedMessage failure = new FailedMessage(
                                    originalFailure.originalMessage(), parsedMessage, realException);
                            newFailures.add(failure);
                        } catch (InterruptedException | TimeoutException e) {
                            Message parsedMessage = originalFailure.parsedMessage().get();

                            log.warn("Timed out reprocessing message (retry attempt #" + retryAttempt +
                                    "): " + parsedMessage, e);

                            suppressPreviousFailureInNewException(originalFailure, e);
                            FailedMessage failure = new FailedMessage(
                                    originalFailure.originalMessage(), parsedMessage, e);
                            newFailures.add(failure);
                        }
                    }

                    log.debug("Retry attempt #{} successful! Processed {} messages on route {}: {}",
                            retryAttempt, reprocessedSuccessfully.size(), routeId, reprocessedSuccessfully);

                    // Give new failures another shot or dead letter them.
                    exchange.getIn().setBody(newFailures);
                })
            .end() // end delay -- see comment below delay(...).
        .end() // end loop
        // If we still have failures, dead letter them.
        .filter(exchangeHasFailures())
        .to(deadLetterUri);
    }

    /**
     * In the event a messages fails on subsequent retries, this tracks that previous failure as a
     * suppressed exception in the latest failure, keeping the history of failures for debugging.
     *
     * <p>Makes sure the exceptions are not referring to the same object to avoid a infinite
     * recursion.
     */
    private void suppressPreviousFailureInNewException(FailedMessage failure, Throwable e) {
        Throwable previousException = failure.exception();

        if (e != previousException) {
            e.addSuppressed(previousException);

            for (Throwable previousSuppressed : previousException.getSuppressed()) {
                e.addSuppressed(previousSuppressed);
            }
        }
    }

    private Predicate maxRetryCountMet() {
        return new Predicate() {
            @Override
            public boolean matches(Exchange exchange) {
                Integer nextAttemptNumber = Optional.ofNullable(
                        exchange.getProperty(NEXT_ATTEMPT_NUMBER_PROPERTY, Integer.class))
                        .orElse(FIRST_ATTEMPT_NUMBER);

                return nextAttemptNumber - FIRST_ATTEMPT_NUMBER >= maxRetryCount;
            }
        };
    }

    private Predicate exchangeHasFailures() {
        return new Predicate() {
            @Override
            public boolean matches(Exchange exchange) {
                Collection failures = exchange.getIn().getBody(Collection.class);

                if (failures == null || failures.isEmpty()) {
                    return false;
                }

                return true;
            }
        };
    }

    private static final class ReprocessingFailure {
        private final FailedMessage originalFailure;
        private final Future<Void> reprocessingFuture;

        private ReprocessingFailure(FailedMessage originalFailure, Future<Void> reprocessingFuture) {
            this.originalFailure = originalFailure;
            this.reprocessingFuture = reprocessingFuture;
        }

        @Override
        public String toString() {
            return "ReprocessingFailure{" +
                    "originalFailure=" + originalFailure +
                    '}';
        }
    }
}
