/*
 *  Copyright 2015 esbtools Contributors and/or its affiliates.
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

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

public final class FailedNotification {
    private final Notification notification;
    private final Collection<String> errors;

    public FailedNotification(Notification notification, Collection<String> errors) {
        this.notification = notification;
        this.errors = Collections.unmodifiableCollection(errors);
    }

    public Notification notification() {
        return notification;
    }

    public Collection<String> errors() {
        return errors;
    }

    @Override
    public String toString() {
        return "FailedNotification{" +
                "notification=" + notification +
                ", errors=" + errors +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FailedNotification that = (FailedNotification) o;
        return Objects.equals(notification, that.notification) &&
                Objects.equals(errors, that.errors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(notification, errors);
    }
}
