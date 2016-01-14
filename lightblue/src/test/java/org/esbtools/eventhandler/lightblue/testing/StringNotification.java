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

package org.esbtools.eventhandler.lightblue.testing;

import com.google.common.util.concurrent.Futures;
import org.esbtools.eventhandler.DocumentEvent;
import org.esbtools.eventhandler.lightblue.LightblueNotification;
import org.esbtools.eventhandler.lightblue.LightblueRequester;
import org.esbtools.lightbluenotificationhook.NotificationEntity;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.Future;

public class StringNotification implements LightblueNotification {
    private final NotificationEntity entity;
    private final String value;
    private final Clock clock;

    public StringNotification(String value, NotificationEntity.Operation operation,
            String triggeredByUser, Clock clock) {
        this.value = value;
        this.clock = clock;

        entity = new NotificationEntity();
        entity.setStatus(NotificationEntity.Status.unprocessed);
        entity.setEntityName("String");
        entity.setEntityVersion("1.0.0");
        entity.setOccurrenceDate(Date.from(clock.instant()));
        entity.setEntityData(Arrays.asList(new NotificationEntity.PathAndValue("value", value)));
        entity.setOperation(operation);
        entity.setTriggeredByUser(triggeredByUser);
    }

    public StringNotification(NotificationEntity entity, LightblueRequester requester) {
        this.entity = entity;
        this.value = entity.getEntityDataForField("value");
        this.clock = Clock.systemUTC();
    }

    @Override
    public Future<Collection<DocumentEvent>> toDocumentEvents() {
        return Futures.immediateFuture(Collections.singleton(new StringDocumentEvent(value, clock)));
    }

    @Override
    public NotificationEntity wrappedNotificationEntity() {
        return entity;
    }
}
