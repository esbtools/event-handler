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

import com.redhat.lightblue.client.Literal;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.Query.BinOp;
import com.redhat.lightblue.client.Update;
import com.redhat.lightblue.client.request.LightblueRequest;
import com.redhat.lightblue.client.request.data.DataUpdateRequest;

import java.sql.Date;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public abstract class UpdateRequests {
    public static DataUpdateRequest notificationsAsProcessing(
            NotificationEntity[] notificationEntities) {
        return null;
    }

    public static DataUpdateRequest processingNotificationsAsProcessed(
            Collection<NotificationEntity> notificationEntities) {
        return null;
    }

    public static Collection<DataUpdateRequest> documentEventsStatusAndProcessedDate(
            Collection<DocumentEventEntity> updatedEventEntities) {
        List<DataUpdateRequest> requests = new ArrayList<>(updatedEventEntities.size());

        for (DocumentEventEntity entity : updatedEventEntities) {
            DataUpdateRequest request = new DataUpdateRequest(
                    DocumentEventEntity.ENTITY_NAME,
                    DocumentEventEntity.VERSION);

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

    public static DataUpdateRequest processingDocumentEventsAsProcessed(
            Collection<DocumentEventEntity> published) {
        return null;
    }

    public static DataUpdateRequest processingDocumentEventsAsFailed(
            Collection<DocumentEventEntity> failed) {
        return null;
    }

    public static DataUpdateRequest processingNotificationsAsFailed(
            List<NotificationEntity> failed) {
        return null;
    }
}
