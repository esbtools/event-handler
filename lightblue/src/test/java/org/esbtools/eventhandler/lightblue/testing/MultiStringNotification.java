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

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.Futures;
import org.esbtools.eventhandler.DocumentEvent;
import org.esbtools.eventhandler.lightblue.LightblueNotification;
import org.esbtools.eventhandler.lightblue.client.LightblueRequester;
import org.esbtools.lightbluenotificationhook.NotificationEntity;

import java.sql.Date;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

public class MultiStringNotification implements LightblueNotification {
    private final NotificationEntity entity;
    private final List<String> values;
    private final Clock clock;

    public MultiStringNotification(List<String> values, NotificationEntity.Operation operation,
            String triggeredByUser, Clock clock) {
        this.values = values;
        this.clock = clock;

        entity = new NotificationEntity();
        entity.setEntityName("MultiString");
        entity.setEntityData(Arrays.asList(
                new NotificationEntity.PathAndValue("values", Joiner.on('|').join(values))));
        entity.setStatus(NotificationEntity.Status.unprocessed);
        entity.setClientRequestPrincipal(triggeredByUser);
        entity.setOperation(operation);
        entity.setEntityVersion("1.0.0");
        entity.setClientRequestDate(Date.from(clock.instant()));
    }

    public MultiStringNotification(NotificationEntity entity, LightblueRequester requester) {
        this.entity = entity;
        this.clock = Clock.systemUTC();
        this.values = Arrays.asList(entity.getEntityDataForField("values").split("\\|"));
    }

    @Override
    public NotificationEntity wrappedNotificationEntity() {
        return entity;
    }

    @Override
    public Future<Collection<DocumentEvent>> toDocumentEvents() {
        return Futures.immediateFuture(Arrays.asList(
                new MultiStringDocumentEvent(entity.get_id(), values, clock)));
    }
}
