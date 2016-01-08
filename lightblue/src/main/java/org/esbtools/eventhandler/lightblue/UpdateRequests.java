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

import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.Query.BinOp;
import com.redhat.lightblue.client.Update;
import com.redhat.lightblue.client.request.data.DataUpdateRequest;

import org.esbtools.eventhandler.lightblue.model.DocumentEventEntity;
import org.esbtools.lightbluenotificationhook.NotificationEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

public abstract class UpdateRequests {
    private static Logger logger = LoggerFactory.getLogger(UpdateRequests.class);

    public static Collection<DataUpdateRequest> notificationsStatusAndProcessedDate(
            Collection<NotificationEntity> updatedNotificationEntities) {
        List<DataUpdateRequest> requests = new ArrayList<>(updatedNotificationEntities.size());

        for (NotificationEntity entity : updatedNotificationEntities) {
            DataUpdateRequest request = new DataUpdateRequest(
                    NotificationEntity.ENTITY_NAME,
                    NotificationEntity.ENTITY_VERSION);

            if (entity.get_id() == null) {
                logger.warn("Tried to update an entity's status and processed date, but entity " +
                        "has no id. Entity was: " + entity);
                continue;
            }

            request.where(Query.withValue("_id", BinOp.eq, entity.get_id()));

            List<Update> updates = new ArrayList<>(2);
            updates.add(Update.set("status", entity.getStatus().toString()));

            Date processedDate = entity.getProcessedDate();

            if (processedDate != null) {
                updates.add(Update.set("processedDate", processedDate));
            }

            // Work around client bug.
            request.updates(updates.toArray(new Update[updates.size()]));

            requests.add(request);
        }

        return requests;
    }

    public static Collection<DataUpdateRequest> documentEventsStatusAndProcessedDate(
            Collection<DocumentEventEntity> updatedEventEntities) {
        List<DataUpdateRequest> requests = new ArrayList<>(updatedEventEntities.size());

        for (DocumentEventEntity entity : updatedEventEntities) {
            DataUpdateRequest request = new DataUpdateRequest(
                    DocumentEventEntity.ENTITY_NAME,
                    DocumentEventEntity.VERSION);

            if (entity.get_id() == null) {
                logger.warn("Tried to update an entity's status and processed date, but entity " +
                        "has no id. Entity was: " + entity);
                continue;
            }

            request.where(Query.withValue("_id", BinOp.eq, entity.get_id()));

            List<Update> updates = new ArrayList<>(2);
            updates.add(Update.set("status", entity.getStatus().toString()));

            ZonedDateTime processedDate = entity.getProcessedDate();

            if (processedDate != null) {
                updates.add(Update.set("processedDate", Date.from(processedDate.toInstant())));
            }

            // Work around client bug.
            request.updates(updates.toArray(new Update[updates.size()]));

            requests.add(request);
        }

        return requests;
    }
}