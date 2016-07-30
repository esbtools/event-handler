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

package org.esbtools.eventhandler.lightblue;

import com.google.common.util.concurrent.Futures;
import org.esbtools.eventhandler.DocumentEvent;
import org.esbtools.lightbluenotificationhook.NotificationEntity;

import java.util.Collection;
import java.util.concurrent.Future;

public class UnparseableNotification implements LightblueNotification {
    private final Exception exception;
    private final NotificationEntity entity;

    public UnparseableNotification(Exception exception, NotificationEntity entity) {
        this.exception = exception;
        this.entity = entity;
    }

    @Override
    public NotificationEntity wrappedNotificationEntity() {
        return entity;
    }

    @Override
    public Future<Collection<DocumentEvent>> toDocumentEvents() {
        return Futures.immediateFailedFuture(exception);
    }
}
