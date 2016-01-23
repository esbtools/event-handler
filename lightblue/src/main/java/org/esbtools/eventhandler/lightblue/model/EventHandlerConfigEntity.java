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

package org.esbtools.eventhandler.lightblue.model;

import io.github.alechenninger.lightblue.Description;
import io.github.alechenninger.lightblue.EntityName;
import io.github.alechenninger.lightblue.Identity;
import io.github.alechenninger.lightblue.Required;
import io.github.alechenninger.lightblue.Version;
import org.esbtools.eventhandler.lightblue.LightblueDocumentEventRepositoryConfig;
import org.esbtools.eventhandler.lightblue.LightblueNotificationRepositoryConfig;

import java.util.Set;

@EntityName(EventHandlerConfigEntity.ENTITY_NAME)
@Version(value = EventHandlerConfigEntity.ENTITY_VERSION, changelog = "Simple, string value only")
public class EventHandlerConfigEntity implements LightblueNotificationRepositoryConfig,
        LightblueDocumentEventRepositoryConfig {
    public static final String ENTITY_NAME = "eventHandlerConfig";
    public static final String ENTITY_VERSION = "1.0.0";

    private String domain;
    private Set<String> canonicalTypesToProcess;
    private Integer documentEventsBatchSize;
    private Set<String> entityNamesToProcess;

    public String getDomain() {
        return domain;
    }

    @Identity
    @Required
    @Description("Identifies a set of configuration values. Since all config is contained in a " +
            "single document in a collection, it is necessary to know how to refer to that " +
            "document: you refer to it by its domain.")
    public void setDomain(String domain) {
        this.domain = domain;
    }

    @Override
    public Set<String> getCanonicalTypesToProcess() {
        return canonicalTypesToProcess;
    }

    @Description("Governs whether or not document events are processed based on their type.")
    public void setCanonicalTypesToProcess(Set<String> canonicalTypesToProcess) {
        this.canonicalTypesToProcess = canonicalTypesToProcess;
    }

    @Override
    public Integer getDocumentEventsBatchSize() {
        return documentEventsBatchSize;
    }

    @Description("Not to be confused with the maximum number of document events passed to " +
            "DocumentEventRepository.retrievePriorityDocumentEventsUpTo(int), this governs the " +
            "max batch size of events fetched from lightblue and available for optimization." +
            "\n" +
            "For example, if you ask for 50 document events to be retrieved, and your batch size " +
            "is 100, we will initially fetch 100 document events (assuming there are >= 100 " +
            "events waiting to be processed) from lightblue. Among those 100, we will try to " +
            "optimize away as many events as possible by checking for events which can be " +
            "merged or superseded. Finally, among those left, we will return the 50 highest " +
            "priority events. Any remaining events past 50 will be untouched, available for " +
            "future retrievals.")
    public void setDocumentEventsBatchSize(int documentEventsBatchSize) {
        this.documentEventsBatchSize = documentEventsBatchSize;
    }

    @Override
    public Set<String> getEntityNamesToProcess() {
        return entityNamesToProcess;
    }

    @Description("Governs whether or not notifications are processed based on their associated " +
            "entity's name.")
    public void setEntityNamesToProcess(Set<String> entityNamesToProcess) {
        this.entityNamesToProcess = entityNamesToProcess;
    }
}
