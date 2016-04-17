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

import org.esbtools.eventhandler.lightblue.LightblueNotificationRepositoryConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@ThreadSafe
public class MutableLightblueNotificationRepositoryConfig implements LightblueNotificationRepositoryConfig {
    private Set<String> entityNamesToProcess;
    private Duration processingTimeout;
    private Duration expireThreshold;

    private static final Logger log = LoggerFactory.getLogger(MutableLightblueNotificationRepositoryConfig.class);

    /**
     * Uses empty default values, which will configure a repository to never retrieve anything.
     */
    public MutableLightblueNotificationRepositoryConfig() {
        this.entityNamesToProcess = Collections.emptySet();
        this.processingTimeout = Duration.ofMinutes(10);
        this.expireThreshold = Duration.ofMinutes(2);}

    /**
     * Uses provided as initial values.
     */
    public MutableLightblueNotificationRepositoryConfig(
            Collection<String> initialEntityNamesToProcess, Duration processingTimeout,
            Duration expireThreshold) {
        this.processingTimeout = Objects.requireNonNull(processingTimeout, "processingTimeout");
        this.expireThreshold = Objects.requireNonNull(expireThreshold, "expireThreshold");
        this.entityNamesToProcess = Collections.unmodifiableSet(new HashSet<>(
                Objects.requireNonNull(initialEntityNamesToProcess, "initialEntityNamesToProcess")));
    }

    @Override
    public Set<String> getEntityNamesToProcess() {
        return entityNamesToProcess;
    }

    public MutableLightblueNotificationRepositoryConfig setEntityNamesToProcess(
            Collection<String> entityNames) {
        Set<String> old = entityNamesToProcess;
        entityNamesToProcess = Collections.unmodifiableSet(new HashSet<>(entityNames));

        if (!old.equals(entityNamesToProcess)) {
            List<String> removed = old.stream()
                    .filter(oldEntityName -> !entityNamesToProcess.contains(oldEntityName))
                    .collect(Collectors.toList());
            List<String> added = entityNamesToProcess.stream()
                    .filter(newEntityName -> !old.contains(newEntityName))
                    .collect(Collectors.toList());
            log.info("Entity names to process updated. Removed {}. Added {}. " +
                    "Currently processing {}.", removed, added, entityNamesToProcess);
        }

        return this;
    }

    @Override
    public Duration getNotificationProcessingTimeout() {
        return processingTimeout;
    }

    public MutableLightblueNotificationRepositoryConfig setNotificationProcessingTimeout(
            Duration notificationProcessingTimeout) {
        Duration old = processingTimeout;
        this.processingTimeout = notificationProcessingTimeout;
        if (!old.equals(processingTimeout)) {
            log.info("Notification processing timeout updated." +
                    " Old value was {}. New value is {}.", old, processingTimeout);
        }
        return this;
    }

    @Override
    public Duration getNotificationExpireThreshold() {
        return expireThreshold;
    }

    public MutableLightblueNotificationRepositoryConfig setNotificationExpireThreshold(
            Duration notificationExpireThreshold) {
        Duration old = expireThreshold;
        this.expireThreshold = notificationExpireThreshold;
        if (!old.equals(expireThreshold)) {
            log.info("Notification expire threshold updated." +
                    " Old value was {}. New value is {}.", old, expireThreshold);
        }
        return this;
    }
}
