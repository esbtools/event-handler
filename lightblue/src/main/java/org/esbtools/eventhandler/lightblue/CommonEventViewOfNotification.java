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

package org.esbtools.eventhandler.lightblue;

import org.esbtools.eventhandler.lightblue.model.DocumentEventEntity;
import org.esbtools.lightbluenotificationhook.NotificationEntity;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CommonEventViewOfNotification implements CommonEventView {
    private final NotificationEntity notificationEntity;

    public CommonEventViewOfNotification(LightblueNotification notification) {
        this.notificationEntity =  notification.wrappedNotificationEntity();
    }

    @Override
    public String entityName() {
        return notificationEntity.getEntityName();
    }

    @Override
    public String entityVersion() {
        return notificationEntity.getEntityVersion();
    }

    @Override
    public List<DocumentEventEntity.KeyAndValue> entityIdentity() {
        return notificationEntity.getEntityIdentity()
                .stream()
                .map(i -> new DocumentEventEntity.KeyAndValue(i.getPath(), i.getValue()))
                .collect(Collectors.toList());
    }

    @Override
    public Optional<Instant> occurrenceDate() {
        return Optional.of(notificationEntity.getOccurrenceDate());
    }

    @Override
    public Optional<Instant> publishDate() {
        return Optional.empty();
    }
    @Override
    public Optional<DocumentEventEntity> documentEvent() {
        return Optional.empty();
    }
}
