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

public class AsyncMessageProcessorRoute extends RouteBuilder {
    private final String fromUri;
    private final String failureUri;
    private final Duration processTimeout;
    private final MessageFactory messageFactory;

    private final int idCount = idCounter.get();
    private final String routeId = "async-message-repository-" + idCount;

    private static final AtomicInteger idCounter = new AtomicInteger(0);

    public AsyncMessageProcessorRoute(String fromUri, String failureUri, Duration processTimeout,
            MessageFactory messageFactory) {
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
                        "the uri <" + fromUri + "> returned: " + exchangeBody);
            }

            Collection messages = (Collection) exchangeBody;

            List<MessageAndProcessingFuture> processingMessages = new ArrayList<>(messages.size());
            List<FailedMessage> failures = new ArrayList<>();

            // Start processing all of the messages in the batch in parallel.
            for (Object message : messages) {
                try {
                    Message parsedMessage = messageFactory.getMessageForBody(message);
                    Future<?> processingFuture = parsedMessage.process();
                    processingMessages.add(new MessageAndProcessingFuture(parsedMessage, processingFuture));
                } catch (Exception e) {
                    log.error("Failure parsing message. Body was: " + message, e);
                    // TODO: How to treat this failure?
                    // We don't have a Message but FailedMessage requires one.
                    // Type-unsafe Failure type?
                }
            }

            // Wait for processing to complete.
            for (MessageAndProcessingFuture processingMsg : processingMessages) {
                try {
                    processingMsg.future.get(processTimeout.toMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException | TimeoutException e) {
                    RecoverableException recoverableException = new RecoverableException(e);
                    failures.add(new FailedMessage(processingMsg.message, recoverableException));
                } catch (ExecutionException e) {
                    failures.add(new FailedMessage(processingMsg.message, e.getCause()));
                }
            }

            // Deal with failures...
            exchange.getIn().setBody(failures);
        })
        .to(failureUri);
    }

    /** Simple struct for storing a message and its future processing result. */
    private static class MessageAndProcessingFuture {
        final Message message;
        final Future<?> future;

        MessageAndProcessingFuture(Message message, Future<?> future) {
            this.message = message;
            this.future = future;
        }
    }
}
