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

import org.esbtools.eventhandler.lightblue.model.EventHandlerConfigEntity;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import org.apache.camel.builder.RouteBuilder;

import java.time.Duration;
import java.util.Set;

/**
 * Updates configuration from stored values in a lightblue instance.
 */
public class PollingLightblueConfigUpdateRoute extends RouteBuilder {
    private final String configDomain;
    private final Duration pollingInterval;
    private final LightblueClient lightblue;
    private final MutableLightblueNotificationRepositoryConfig notificationRepositoryConfig;
    private final MutableLightblueDocumentEventRepositoryConfig documentEventRepositoryConfig;

    private final DataFindRequest findConfig;

    /**
     * @param configDomain See {@link EventHandlerConfigEntity#setDomain(String)}}. This is whatever
     *                     you use when you persist your configuration.
     * @param pollingInterval How often to poll for configuration data stored in lightblue.
     * @param lightblue A lightblue client configured to talk to lightblue
     * @param notificationRepositoryConfig A thread-safe config object to update
     * @param documentEventRepositoryConfig A thread-safe config object to update
     */
    public PollingLightblueConfigUpdateRoute(String configDomain, Duration pollingInterval,
            LightblueClient lightblue,
            MutableLightblueNotificationRepositoryConfig notificationRepositoryConfig,
            MutableLightblueDocumentEventRepositoryConfig documentEventRepositoryConfig) {
        this.pollingInterval = pollingInterval;
        this.lightblue = lightblue;
        this.notificationRepositoryConfig = notificationRepositoryConfig;
        this.documentEventRepositoryConfig = documentEventRepositoryConfig;
        this.configDomain = configDomain;

        findConfig = FindRequests.eventHandlerConfigForDomain(configDomain);
    }

    @Override
    public void configure() throws Exception {
        from("timer:pollForNotifications?period=" + pollingInterval.toMillis())
        .routeId("lightblue-repository-config-update")
        .process(exchange -> {
            EventHandlerConfigEntity storedConfig =
                    lightblue.data(findConfig, EventHandlerConfigEntity.class);

            if (storedConfig == null) {
                log.info("No event handler config found for domain: {}", configDomain);
                return;
            }

            Set<String> canonicalTypes = storedConfig.getCanonicalTypesToProcess();
            if (canonicalTypes != null) {
                documentEventRepositoryConfig.setCanonicalTypesToProcess(canonicalTypes);
            }

            Set<String> entityNames = storedConfig.getEntityNamesToProcess();
            if (entityNames != null) {
                notificationRepositoryConfig.setEntityNamesToProcess(entityNames);
            }

            Integer documentEventBatchSize = storedConfig.getDocumentEventsBatchSize();
            if (documentEventBatchSize != null) {
                documentEventRepositoryConfig.setDocumentEventsBatchSize(documentEventBatchSize);
            }
        });
    }
}
