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

import com.redhat.lightblue.client.Literal;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.Sort;
import com.redhat.lightblue.client.request.data.DataFindRequest;

import org.esbtools.eventhandler.lightblue.model.DocumentEventEntity;
import org.esbtools.lightbluenotificationhook.NotificationEntity;

public abstract class FindRequests {
    /**
     * You generally don't want to retrieve notifications which have already started being processed
     * or have been processed, so this limits the request to
     * {@link org.esbtools.lightbluenotificationhook.NotificationEntity.Status#unprocessed}
     * notifications.
     */
    public static DataFindRequest oldestNotificationsForEntitiesUpTo(String[] entities,
            int maxEvents) {
        DataFindRequest findEntities = new DataFindRequest(
                NotificationEntity.ENTITY_NAME,
                NotificationEntity.ENTITY_VERSION);

        findEntities.where(Query.and(
                Query.withValues("entityName", Query.NaryOp.in, Literal.values(entities)),
                Query.withValue("status", Query.BinOp.eq, NotificationEntity.Status.unprocessed)));
        findEntities.select(Projection.includeFieldRecursively("*"));
        findEntities.sort(Sort.asc("occurrenceDate"));
        findEntities.range(0, maxEvents - 1);

        return findEntities;
    }

    /**
     * You generally don't want to retrieve document events which have already started being
     * processed or have been processed, so this limits the request to
     * {@link org.esbtools.eventhandler.lightblue.model.DocumentEventEntity.Status#unprocessed}
     * document events.
     */
    public static DataFindRequest priorityDocumentEventsForTypesUpTo(String[] types,
            int maxEvents) {
        DataFindRequest findEntities = new DataFindRequest(DocumentEventEntity.ENTITY_NAME,
                DocumentEventEntity.VERSION);

        findEntities.where(Query.and(
                Query.withValues("canonicalType", Query.NaryOp.in, Literal.values(types)),
                Query.withValue("status", Query.BinOp.eq, DocumentEventEntity.Status.unprocessed)));
        findEntities.select(Projection.includeFieldRecursively("*"));
        findEntities.sort(Sort.desc("priority"));
        findEntities.range(0, maxEvents - 1);

        return findEntities;
    }
}
