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

package org.esbtools.eventhandler.lightblue.config;

import org.esbtools.eventhandler.lightblue.LightblueDocumentEventRepositoryConfig;
import org.esbtools.eventhandler.lightblue.LightblueNotificationRepositoryConfig;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.redhat.lightblue.generator.Description;
import com.redhat.lightblue.generator.EntityName;
import com.redhat.lightblue.generator.Identity;
import com.redhat.lightblue.generator.Required;
import com.redhat.lightblue.generator.Transient;
import com.redhat.lightblue.generator.Version;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;

@EntityName(EventHandlerConfigEntity.ENTITY_NAME)
@Version(value = "0.1.1-SNAPSHOT", preferImplementationVersion = false, changelog = "Initial domain-specific config")
public class EventHandlerConfigEntity implements LightblueNotificationRepositoryConfig,
        LightblueDocumentEventRepositoryConfig {
    public static final String ENTITY_NAME = "eventHandlerConfig";
    public static final String ENTITY_VERSION = Version.FromAnnotation.onEntity(EventHandlerConfigEntity.class);

    private String domain;
    private Set<String> canonicalTypesToProcess;
    private Integer documentEventsBatchSize;
    private Set<String> entityNamesToProcess;
    private Integer notificationProcessingTimeoutSeconds;
    private Integer notificationExpireThresholdSeconds;
    private Integer documentEventProcessingTimeoutSeconds;
    private Integer documentEventExpireThresholdSeconds;
    private Integer maxDocumentEventsPerInsert;

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
    public void setDocumentEventsBatchSize(Integer documentEventsBatchSize) {
        this.documentEventsBatchSize = documentEventsBatchSize;
    }

    @Override
    @Transient
    public Duration getDocumentEventProcessingTimeout() {
        return documentEventProcessingTimeoutSeconds == null
                ? null
                : Duration.ofSeconds(documentEventProcessingTimeoutSeconds);
    }

    public Integer getDocumentEventProcessingTimeoutSeconds() {
        return documentEventProcessingTimeoutSeconds;
    }

    @Description("How long can a document event remain processing before we allow it to be " +
            "retrieved again for reprocessing?")
    public void setDocumentEventProcessingTimeoutSeconds(
            Integer documentEventProcessingTimeoutSeconds) {
        this.documentEventProcessingTimeoutSeconds = documentEventProcessingTimeoutSeconds;
    }

    @Override
    @Transient
    public Duration getDocumentEventExpireThreshold() {
        return documentEventExpireThresholdSeconds == null
                ? null
                : Duration.ofSeconds(documentEventExpireThresholdSeconds);
    }

    public Integer getDocumentEventExpireThresholdSeconds() {
        return documentEventExpireThresholdSeconds;
    }

    @Description("How long before a document event is available for retrieval do we drop the " +
            "event and let it be reprocessed?\n" +
            "In other words, this governs when we stop processing an event in flight because " +
            "we're too near when another retrieval may see it is past its " +
            "getDocumentEventProcessingTimeout() and retrieve it for reprocessing.\n" +
            "N.B. The existence of this configuration is a function of our current transaction " +
            "scheme. This could go away, for instance, if we either atomically updated an " +
            "event's processing timestamp before publishing its document. Other alternative " +
            "schemes are possible.")
    public void setDocumentEventExpireThresholdSeconds(
            Integer documentEventExpireThresholdSeconds) {
        this.documentEventExpireThresholdSeconds = documentEventExpireThresholdSeconds;
    }

    @Override
    @Transient
    @JsonIgnore
    // TODO(ahenning): When metadata generator supports optional, remove @Transient and combine
    // with getMaxDocumentEventsPerInsert
    public Optional<Integer> getOptionalMaxDocumentEventsPerInsert() {
        return Optional.ofNullable(maxDocumentEventsPerInsert);
    }

    public Integer getMaxDocumentEventsPerInsert() {
        return maxDocumentEventsPerInsert;
    }

    @Description("When adding new document events, we can make (total new events) / (max events " +
            "per insert) requests, instead of one request with all new events in a single call. " +
            "If no integer is provided, we will do one request with all new events.\n" +
            "Setting a limit is recommended as it protects against potentially extremely " +
            "significant notifications producing a huge quantity of document events and failing " +
            "to insert them all in one call.")
    public void setMaxDocumentEventsPerInsert(@Nullable Integer maxDocumentEventsPerInsert) {
        this.maxDocumentEventsPerInsert = maxDocumentEventsPerInsert;
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

    @Override
    @Transient
    public Duration getNotificationProcessingTimeout() {
        return notificationProcessingTimeoutSeconds == null
                ? null
                : Duration.ofSeconds(notificationProcessingTimeoutSeconds);
    }

    public Integer getNotificationProcessingTimeoutSeconds() {
        return notificationProcessingTimeoutSeconds;
    }

    @Description("How long can a notification remain processing before we allow it to be " +
            "retrieved again for reprocessing?")
    public void setNotificationProcessingTimeoutSeconds(Integer notificationProcessingTimeoutSeconds) {
        this.notificationProcessingTimeoutSeconds = notificationProcessingTimeoutSeconds;
    }

    @Override
    @Transient
    public Duration getNotificationExpireThreshold() {
        return notificationExpireThresholdSeconds == null
                ? null
                : Duration.ofSeconds(notificationExpireThresholdSeconds);
    }

    public Integer getNotificationExpireThresholdSeconds() {
        return notificationExpireThresholdSeconds;
    }

    @Description("How long before a notification is available for retrieval do we drop the event " +
            "and let it be reprocessed?\n" +
            "In other words, this governs when we stop processing a notification in flight " +
            "because we're too near when another retrieval may see it is past its " +
            "getNotificationProcessingTimeout() and retrieve it for reprocessing.\n" +
            "N.B. The existence of this configuration is a function of our current transaction " +
            "scheme. This could go away, for instance, if we either atomically updated a " +
            "notification's processing timestamp before adding its document events. Other " +
            "alternative schemes are possible.")
    public void setNotificationExpireThresholdSeconds(Integer notificationExpireThresholdSeconds) {
        this.notificationExpireThresholdSeconds = notificationExpireThresholdSeconds;
    }
}
