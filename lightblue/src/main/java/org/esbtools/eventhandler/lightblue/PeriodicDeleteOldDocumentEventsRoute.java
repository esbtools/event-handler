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

import org.esbtools.eventhandler.lightblue.locking.LockNotAvailableException;
import org.esbtools.eventhandler.lightblue.locking.LockStrategy;
import org.esbtools.eventhandler.lightblue.locking.LockedResource;
import org.esbtools.eventhandler.lightblue.locking.LostLockException;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.request.data.DataDeleteRequest;
import com.redhat.lightblue.client.response.LightblueDataResponse;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.support.RoutePolicySupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.Date;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

public class PeriodicDeleteOldDocumentEventsRoute extends RouteBuilder {
    private final LightblueClient client;
    private final LockStrategy lockStrategy;
    private final Duration deleteEventsOlderThan;
    private final Duration deleteInterval;
    private final Clock clock;

    /** Package visible for testing. */
    static final String DELETER_LOCK_RESOURCE_ID = "oldDocumentEventsDeleter";

    private static final Logger log = LoggerFactory.getLogger(PeriodicDeleteOldDocumentEventsRoute.class);

    public PeriodicDeleteOldDocumentEventsRoute(LightblueClient client, LockStrategy lockStrategy,
            Duration deleteEventsOlderThan, Duration deleteInterval, Clock clock) {
        this.client = client;
        this.lockStrategy = lockStrategy;
        this.deleteEventsOlderThan = deleteEventsOlderThan;
        this.deleteInterval = deleteInterval;
        this.clock = clock;
    }

    @Override
    public void configure() throws Exception {
        from("timer:oldDocumentEventsDeleter?period=" + deleteInterval.toMillis())
        .routeId("oldDocumentEventsDeleter")
        .routePolicy(new DeleterLockRoutePolicy())
        .process(exchange -> {
            Instant tooOld = clock.instant().minus(deleteEventsOlderThan);

            log.debug("Deleting document events created before {}", tooOld);

            DataDeleteRequest deleteOldEvents = new DataDeleteRequest(
                    DocumentEventEntity.ENTITY_NAME,
                    DocumentEventEntity.VERSION);
            deleteOldEvents.where(Query.withValue(
                    "creationDate", Query.BinOp.lt, Date.from(tooOld)));

            LightblueDataResponse response = client.data(deleteOldEvents);

            log.info("Deleted {} document events created before {}",
                    response.parseMatchCount(), tooOld);
        });
    }

    private class DeleterLockRoutePolicy extends RoutePolicySupport {
        private @Nullable LockedResource<String> lock;

        @Override
        public void onStop(Route route) {
            releaseLock();
        }

        @Override
        public void onSuspend(Route route) {
            releaseLock();
        }

        @Override
        public synchronized void onExchangeBegin(Route route, Exchange exchange) {
            if (lock != null) {
                try {
                    lock.ensureAcquiredOrThrow("Lost lock");
                    return;
                } catch (LostLockException e) {
                    log.warn("Lost document events deleter lock, trying to reacquire...", e);
                    lock = null;
                }
            }

            try {
                lock = lockStrategy.tryAcquire(DELETER_LOCK_RESOURCE_ID);
            } catch (LockNotAvailableException e) {
                log.debug("Old document events deleter lock not available, assuming " +
                        "another thread is cleaning up old document events.", e);
                exchange.setProperty(Exchange.ROUTE_STOP, Boolean.TRUE);
            }
        }

        private synchronized void releaseLock() {
            if (lock == null) return;

            try {
                lock.close();
            } catch (IOException e) {
                log.warn("IOException trying to release old document events deleter lock", e);
            }

            lock = null;
        }
    }
}
