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

import javax.annotation.concurrent.ThreadSafe;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@ThreadSafe
public class MutableLightblueNotificationRepositoryConfig implements LightblueNotificationRepositoryConfig {
    private Set<String> entityNamesToProcess;
    private Duration processingTimeout;
    private Duration expireThreshold;

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
        entityNamesToProcess = Collections.unmodifiableSet(new HashSet<>(entityNames));
        return this;
    }

    @Override
    public Duration getNotificationProcessingTimeout() {
        return processingTimeout;
    }

    public MutableLightblueNotificationRepositoryConfig setNotificationProcessingTimeout(
            Duration notificationProcessingTimeout) {
        this.processingTimeout = notificationProcessingTimeout;
        return this;
    }

    @Override
    public Duration getNotificationExpireThreshold() {
        return expireThreshold;
    }

    public MutableLightblueNotificationRepositoryConfig setNotificationExpireThreshold(
            Duration notificationExpireThreshold) {
        this.expireThreshold = notificationExpireThreshold;
        return this;
    }
}
