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

public final class FailedMessage {
    private final Message message;
    private final Throwable exception;

    public FailedMessage(Message message, Throwable exception) {
        this.message = message;
        this.exception = exception;
    }

    public Message message() {
        return message;
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
                ", message=" + message +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FailedMessage that = (FailedMessage) o;
        return Objects.equals(message, that.message) &&
                Objects.equals(exception, that.exception);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, exception);
    }
}
