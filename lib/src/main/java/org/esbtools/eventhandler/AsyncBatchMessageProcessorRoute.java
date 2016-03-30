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

import org.apache.camel.builder.RouteBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncBatchMessageProcessorRoute extends RouteBuilder {
    private final String fromUri;
    private final String failureUri;
    private final Duration processTimeout;
    private final MessageFactory messageFactory;

    private final int idCount = idCounter.getAndIncrement();
    private final String routeId = "messageProcessor-" + idCount;

    private static final AtomicInteger idCounter = new AtomicInteger(0);

    /**
     * @param fromUri Endpoint to consume from, expected to create exchanges with bodies instances
     *                of {@link Collection}. The elements of this collection will be provided to
     *                {@code messageFactory} to parse them into {@link Message}s.
     * @param failureUri Endpoint where failures will be sent to as a {@code Collection} of
     *                   {@link FailedMessage}s.
     * @param processTimeout How to long to wait for a message process before timing out? Time outs
     *                       are considered {@link FailedMessage#isRecoverable() recoverable
     *                       failures}.
     * @param messageFactory Accepts each element in the exchange body {@code Collection} and
     *                       parses them to create message implementations which will be processed.
     */
    public AsyncBatchMessageProcessorRoute(String fromUri, String failureUri,
            Duration processTimeout, MessageFactory messageFactory) {
        this.fromUri = Objects.requireNonNull(fromUri, "fromUri");
        this.failureUri = Objects.requireNonNull(failureUri, "failureUri");
        this.processTimeout = Objects.requireNonNull(processTimeout, "processTimeout");
        this.messageFactory = Objects.requireNonNull(messageFactory, "messageFactory");
    }

    @Override
    public void configure() throws Exception {
        from(fromUri)
        .routeId(routeId)
        .process(exchange -> {
            Object exchangeBody = exchange.getIn().getBody();

            if (!(exchangeBody instanceof Collection)) {
                throw new IllegalArgumentException("Expected `fromUri` to deliver exchanges with " +
                        "Collection bodies so that we may batch process for efficiency. However, " +
                        "the uri '" + fromUri + "' returned the " +
                        exchangeBody.getClass().getName() + ": " + exchangeBody);
            }

            Collection originalMessages = (Collection) exchangeBody;

            List<ProcessingMessage> processingMessages = new ArrayList<>(originalMessages.size());
            List<FailedMessage> failures = new ArrayList<>();

            // Start processing all of the messages in the batch in parallel.
            for (Object originalMessage : originalMessages) {
                final Message message;

                try {
                    message = messageFactory.getMessageForBody(originalMessage);
                } catch (Exception e) {
                    log.error("Failure parsing message. Body was: " + originalMessage, e);
                    failures.add(new FailedMessage(originalMessage, e));
                    continue;
                }

                final Future<Void> processingFuture;

                try {
                    processingFuture = message.process();
                } catch (Exception e) {
                    log.error("Failed to process message: " + message, e);
                    FailedMessage failure = new FailedMessage(originalMessage, message, e);
                    failures.add(failure);
                    continue;
                }

                ProcessingMessage processing = new ProcessingMessage(
                        originalMessage, message, processingFuture);
                processingMessages.add(processing);

                log.debug("Processing on route {}: {}", routeId, message);
            }

            // Wait for processing to complete.
            for (ProcessingMessage processingMsg : processingMessages) {
                try {
                    processingMsg.future.get(processTimeout.toMillis(), TimeUnit.MILLISECONDS);
                } catch (ExecutionException e) {
                    log.error("Failed to process message: " + processingMsg.parsedMessage, e);
                    FailedMessage failure = new FailedMessage(processingMsg.originalMessage,
                            processingMsg.parsedMessage, e.getCause());
                    failures.add(failure);
                } catch (InterruptedException | TimeoutException e) {
                    log.warn("Timed out processing message: " + processingMsg.parsedMessage, e);
                    // TODO(ahenning): Consider removing recoverable exception thing
                    RecoverableException recoverableException = new RecoverableException(e);
                    FailedMessage failure = new FailedMessage(processingMsg.originalMessage,
                            processingMsg.parsedMessage, recoverableException);
                    failures.add(failure);
                }
            }

            // Deal with failures...
            exchange.getIn().setBody(failures);
        })
        .to(failureUri);
    }

    /**
     * Simple struct for storing a message and its future processing result.
     */
    private static class ProcessingMessage {
        final Object originalMessage;
        final Message parsedMessage;
        final Future<Void> future;

        ProcessingMessage(Object originalMessage, Message parsedMessage, Future<Void> future) {
            this.originalMessage = originalMessage;
            this.parsedMessage = parsedMessage;
            this.future = future;
        }
    }
}
