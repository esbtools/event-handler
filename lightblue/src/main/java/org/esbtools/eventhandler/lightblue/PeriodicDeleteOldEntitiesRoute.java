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

import java.sql.Date;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import org.apache.camel.builder.RouteBuilder;
import org.esbtools.eventhandler.lightblue.locking.LockStrategy;
import org.esbtools.eventhandler.lightblue.locking.LockingRoutePolicy;
import org.esbtools.lightbluenotificationhook.NotificationEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.request.data.DataDeleteRequest;
import com.redhat.lightblue.client.response.LightblueDataResponse;

public class PeriodicDeleteOldEntitiesRoute extends RouteBuilder {
    private final LightblueClient client;
    private final LockStrategy lockStrategy;
    private final Duration deleteOlderThan;
    private final Duration deleteInterval;
    private final Clock clock;
    private final String domain;
    private final String entityName;
    private final String entityVersion;
    private final String entityDateField;

    /** Package visible for testing. */
    final String deleterLockResourceId;

    private static final Logger log = LoggerFactory.getLogger(PeriodicDeleteOldEntitiesRoute.class);

    public static PeriodicDeleteOldEntitiesRoute deletingNotificationsOlderThan(
            Duration deleteOlderThan, Duration deleteInterval, LightblueClient client,
            String domain, LockStrategy lockStrategy, Clock clock) {
        return new PeriodicDeleteOldEntitiesRoute(domain, NotificationEntity.ENTITY_NAME,
                NotificationEntity.ENTITY_VERSION, "clientRequestDate", client, lockStrategy,
                deleteOlderThan, deleteInterval, clock);
    }

    public static PeriodicDeleteOldEntitiesRoute deletingDocumentEventsOlderThan(
            Duration deleteOlderThan, Duration deleteInterval, LightblueClient client,
            String domain, LockStrategy lockStrategy, Clock clock) {
        return new PeriodicDeleteOldEntitiesRoute( domain, DocumentEventEntity.ENTITY_NAME,
                DocumentEventEntity.VERSION, "creationDate", client, lockStrategy,
                deleteOlderThan, deleteInterval, clock);
    }

    public PeriodicDeleteOldEntitiesRoute(String domain, String entityName, String entityVersion,
            String entityDateField, LightblueClient client, LockStrategy lockStrategy,
            Duration deleteOlderThan, Duration deleteInterval, Clock clock) {
        this.client = client;
        this.lockStrategy = lockStrategy;
        this.deleteOlderThan = deleteOlderThan;
        this.deleteInterval = deleteInterval;
        this.clock = clock;
        this.domain = domain;
        this.entityName = entityName;
        this.entityVersion = entityVersion;
        this.entityDateField = entityDateField;

        deleterLockResourceId = "old_" + domain + "_" + entityName + "_deleter";
    }

    @Override
    public void configure() throws Exception {
        from("timer:" + deleterLockResourceId + "?period=" + deleteInterval.toMillis())
        .routeId(deleterLockResourceId)
        .routePolicy(new LockingRoutePolicy(deleterLockResourceId, lockStrategy))
        .process(exchange -> {
            Instant tooOld = clock.instant().minus(deleteOlderThan);

            log.debug("Deleting {} entities with {} before {}", entityName, entityDateField, tooOld);

            DataDeleteRequest deleteRequest = new DataDeleteRequest(entityName, entityVersion);
            deleteRequest.where(Query.withValue(entityDateField, Query.BinOp.lt, Date.from(tooOld)));

            LightblueDataResponse response = client.data(deleteRequest);

            log.info("Deleted {} {} entities with {} before {}",
                    response.parseModifiedCount(), entityName, entityDateField, tooOld);
        });
    }


}
