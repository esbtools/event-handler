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

import java.util.Objects;
import java.util.Optional;

public final class FailedMessage {
    private final Object originalMessage;
    private final Optional<Message> parsedMessage;
    private final Throwable exception;

    public FailedMessage(Object originalMessage, Message parsedMessage, Throwable exception) {
        this.originalMessage = originalMessage;
        this.parsedMessage = Optional.of(parsedMessage);
        this.exception = exception;
    }

    public FailedMessage(Object originalMessage, Throwable exception) {
        this.originalMessage = originalMessage;
        this.parsedMessage = Optional.empty();
        this.exception = exception;
    }

    public Object originalMessage() {
        return originalMessage;
    }

    public Optional<Message> parsedMessage() {
        return parsedMessage;
    }

    public Throwable exception() {
        return exception;
    }

    public boolean isRecoverable() {
        return exception instanceof RecoverableException;
    }

    @Override
    public String toString() {
        return "FailedMessage{" +
                "exception=" + exception +
                ", parsedMessage=" + parsedMessage +
                ", originalMessage=" + originalMessage +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FailedMessage that = (FailedMessage) o;
        return Objects.equals(originalMessage, that.originalMessage) &&
                Objects.equals(parsedMessage, that.parsedMessage) &&
                Objects.equals(exception, that.exception);
    }

    @Override
    public int hashCode() {
        return Objects.hash(originalMessage, parsedMessage, exception);
    }
}
