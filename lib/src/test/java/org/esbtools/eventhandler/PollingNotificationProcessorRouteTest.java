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

package org.esbtools.eventhandler;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.esbtools.eventhandler.testing.SimpleInMemoryDocumentEventRepository;
import org.esbtools.eventhandler.testing.SimpleInMemoryNotificationRepository;
import org.junit.Test;

import java.time.Duration;

public class PollingNotificationProcessorRouteTest extends CamelTestSupport {
    SimpleInMemoryNotificationRepository notificationRepository = new SimpleInMemoryNotificationRepository();
    SimpleInMemoryDocumentEventRepository documentEventRepository = new SimpleInMemoryDocumentEventRepository();

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new PollingNotificationProcessorRoute(notificationRepository, documentEventRepository,
                Duration.ofSeconds(1), 10);
    }

    @Test
    public void shouldTurnNotificationsIntoDocumentEventsAndPersistAtPeroidicIntervals() {

    }
}
