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

package org.esbtools.eventhandler.lightblue.client;

import org.esbtools.eventhandler.lightblue.DocumentEventEntity;
import org.esbtools.eventhandler.lightblue.config.EventHandlerConfigEntity;
import org.esbtools.lightbluenotificationhook.NotificationEntity;

import com.redhat.lightblue.client.Literal;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.Sort;
import com.redhat.lightblue.client.request.data.DataFindRequest;

import java.time.Instant;
import java.util.Date;

public abstract class FindRequests {
    /**
     * Constructs a find request which retrieves up to {@code maxNotifications} notifications of the
     * given {@code entityNames} which are either currently
     * {@link NotificationEntity.Status#unprocessed} or expired.
     *
     * <p>Notifications are expired when their {@link NotificationEntity#getProcessingDate()} is at
     * or older than the provided {@code expiredProcessingDate}.
     */
    public static DataFindRequest oldestNotificationsForEntitiesUpTo(String[] entityNames,
            int maxNotifications, Instant expiredProcessingDate) {
        DataFindRequest findEntities = new DataFindRequest(
                NotificationEntity.ENTITY_NAME,
                NotificationEntity.ENTITY_VERSION);

        findEntities.where(Query.and(
                Query.withValue("clientRequestDate", Query.neq, Literal.value(null)),
                Query.withValues("entityName", Query.NaryOp.in, Literal.values(entityNames)),
                Query.or(
                        Query.withValue("status", Query.BinOp.eq, DocumentEventEntity.Status.unprocessed),
                        Query.and(
                                Query.withValue("status", Query.BinOp.eq, DocumentEventEntity.Status.processing),
                                Query.withValue("processingDate", Query.BinOp.lte, Date.from(expiredProcessingDate)))
                )));
        findEntities.select(Projection.includeFieldRecursively("*"));
        findEntities.sort(Sort.asc("clientRequestDate"));
        findEntities.range(0, maxNotifications - 1);

        return findEntities;
    }

    /**
     * Constructs a find request which retrieves up to {@code maxEvents} events of the given
     * {@code types} which are either currently {@link DocumentEventEntity.Status#unprocessed} or
     * expired.
     *
     * <p>Events are expired when their {@link DocumentEventEntity#getProcessingDate()} is at or
     * older than the provided {@code expiredProcessingDate}.
     */
    public static DataFindRequest priorityDocumentEventsForTypesUpTo(String[] types,
        int maxEvents, Instant expiredProcessingDate) {
        DataFindRequest findEntities = new DataFindRequest(DocumentEventEntity.ENTITY_NAME,
                DocumentEventEntity.VERSION);

        findEntities.where(Query.and(
                Query.withValues("canonicalType", Query.NaryOp.in, Literal.values(types)),
                Query.or(
                        Query.withValue("status", Query.BinOp.eq, DocumentEventEntity.Status.unprocessed),
                        Query.and(
                                Query.withValue("status", Query.BinOp.eq, DocumentEventEntity.Status.processing),
                                Query.withValue("processingDate", Query.BinOp.lte, Date.from(expiredProcessingDate)))
                )));
        findEntities.select(Projection.includeFieldRecursively("*"));
        findEntities.sort(Sort.desc("priority"), Sort.asc("creationDate"));
        findEntities.range(0, maxEvents - 1);

        return findEntities;
    }

    public static DataFindRequest eventHandlerConfigForDomain(String configDomain) {
        DataFindRequest findConfig = new DataFindRequest(
                EventHandlerConfigEntity.ENTITY_NAME,
                EventHandlerConfigEntity.ENTITY_VERSION);

        findConfig.where(Query.withValue("domain", Query.BinOp.eq, configDomain));
        findConfig.select(Projection.includeFieldRecursively("*"));

        return findConfig;
    }
}
