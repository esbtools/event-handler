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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RetryingFailedMessageRoute extends RouteBuilder {
    private final String fromUri;
    private final Expression retryDelay;
    private final int maxRetryCount;
    private final Duration processTimeout;
    private final String deadLetterUri;

    private static final String NEXT_RETRY_ATTEMPT_NUMBER_PROPERTY = "eventHandlerFailedMessageNextRetryAttemptNumber";
    private static final Integer FIRST_RETRY_ATTEMPT_NUMBER = 1;

    public RetryingFailedMessageRoute(String fromUri, Expression retryDelay, int maxRetryCount,
            Duration processTimeout, String deadLetterUri) {
        this.fromUri = fromUri;
        this.retryDelay = retryDelay;
        this.maxRetryCount = maxRetryCount;
        this.processTimeout = processTimeout;
        this.deadLetterUri = deadLetterUri;
    }

    @Override
    public void configure() throws Exception {
        from(fromUri)
        // We use loop instead of error handler because error handlers start with original message
        // sent to point of failure. We need to continuously process until there are no more
        // remaining failures. We don't want to reprocess messages which already succeeded.
        .loopDoWhile(and(
                exchangeHasFailures(),
                not(maxRetryCountMet())))
            .delay(retryDelay)
            .process(exchange -> {
                Integer retryAttempt = Optional.of(
                        exchange.getProperty(NEXT_RETRY_ATTEMPT_NUMBER_PROPERTY, Integer.class))
                        .orElse(FIRST_RETRY_ATTEMPT_NUMBER);
                // Preemptively increment retry attempt for next loop.
                exchange.setProperty(NEXT_RETRY_ATTEMPT_NUMBER_PROPERTY, retryAttempt + 1);

                Collection oldFailures = exchange.getIn().getMandatoryBody(Collection.class);
                List<FailedMessage> newFailures = new ArrayList<>();
                List<ReprocessingFailure> reprocessingFailures = new ArrayList<>(oldFailures.size());

                // Begin processing all failed messages again in parallel.
                for (Object failureAsObject : oldFailures) {
                    if (!(failureAsObject instanceof FailedMessage)) {
                        throw new IllegalArgumentException("Messages sent to " + RetryingFailedMessageRoute.class +
                                "route should be collections of FailedMessage elements, but got " +
                                "collection of " + failureAsObject.getClass());
                    }

                    FailedMessage failure = (FailedMessage) failureAsObject;
                    Optional<Message> maybeMessage = failure.parsedMessage();

                    if (!maybeMessage.isPresent()) {
                        // Nothing to retry; dead letter it
                        // This happens when message factory failed to get message from original body.
                        // We won't bother trying get the message from the message factory again; if
                        // that fails it is usually a bug that retrying won't circumvent.
                        continue;
                    }

                    Message message = maybeMessage.get();
                    final Future<Void> reprocessingFuture;

                    try {
                        reprocessingFuture = message.process();
                    } catch (Exception e) {
                        log.error("Failed to reprocess message (retry attempt " + retryAttempt + "): " + message, e);
                        // Track the previous try's exception.
                        e.addSuppressed(failure.exception());
                        newFailures.add(new FailedMessage(failure.originalMessage(), message, e));
                        continue;
                    }

                    reprocessingFailures.add(new ReprocessingFailure(failure, reprocessingFuture));
                }

                for (ReprocessingFailure reprocessingFailure : reprocessingFailures) {
                    FailedMessage originalFailure = reprocessingFailure.originalFailure;

                    try {
                        reprocessingFailure.reprocessingFuture
                                .get(processTimeout.toMillis(), TimeUnit.MILLISECONDS);
                    } catch (ExecutionException e) {
                        Message parsedMessage = originalFailure.parsedMessage().get();

                        log.error("Failed to reprocess message (retry attempt " + retryAttempt +
                                "): " + parsedMessage, e);

                        // Track the previous try's exception.
                        e.getCause().addSuppressed(originalFailure.exception());

                        FailedMessage failure = new FailedMessage(originalFailure.originalMessage(),
                                parsedMessage, e.getCause());
                        newFailures.add(failure);
                    } catch (InterruptedException | TimeoutException e) {
                        Message parsedMessage = originalFailure.parsedMessage().get();

                        // Track the previous try's exception.
                        e.getCause().addSuppressed(originalFailure.exception());

                        log.warn("Timed out reprocessing message (retry attempt " + retryAttempt +
                                "): " + parsedMessage, e);

                        // TODO(ahenning): Consider removing recoverable exception thing
                        e.addSuppressed(originalFailure.exception());
                        RecoverableException recoverableException = new RecoverableException(e);
                        FailedMessage failure = new FailedMessage(originalFailure.originalMessage(),
                                parsedMessage, recoverableException);
                        newFailures.add(failure);
                    }
                }

                // Give new failures another shot or dead letter them.
                exchange.getIn().setBody(newFailures);
            })
        .end()
        .to(deadLetterUri);
    }

    private Predicate maxRetryCountMet() {
        return new Predicate() {
            @Override
            public boolean matches(Exchange exchange) {
                Integer retryAttempt = Optional.of(
                        exchange.getProperty(NEXT_RETRY_ATTEMPT_NUMBER_PROPERTY, Integer.class))
                        .orElse(FIRST_RETRY_ATTEMPT_NUMBER);

                if (retryAttempt - FIRST_RETRY_ATTEMPT_NUMBER >= maxRetryCount) {
                    return true;
                }

                return false;
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
    }
}
