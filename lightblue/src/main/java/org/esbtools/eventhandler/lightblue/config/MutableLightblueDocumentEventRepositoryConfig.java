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
public class MutableLightblueDocumentEventRepositoryConfig implements LightblueDocumentEventRepositoryConfig {
    private Set<String> canonicalTypesToProcess = Collections.emptySet();
    private int documentEventsBatchSize = 0;
    private Duration processingTimeout = Duration.ofMinutes(10);
    private Duration expireThreshold = Duration.ofMinutes(2);

    private static final Logger log = LoggerFactory.getLogger(MutableLightblueDocumentEventRepositoryConfig.class);

    /**
     * Uses empty default values, which will configure a repository to never retrieve anything.
     */
    public MutableLightblueDocumentEventRepositoryConfig() {
        this.canonicalTypesToProcess = Collections.emptySet();
        this.documentEventsBatchSize = 0;
        this.processingTimeout = Duration.ofMinutes(10);
        this.expireThreshold = Duration.ofMinutes(2);
    }

    /**
     * Uses provided as initial values.
     */
    public MutableLightblueDocumentEventRepositoryConfig(
            Collection<String> initialCanonicalTypesToProcess,
            int initialDocumentEventsBatchSize, Duration processingTimeout,
            Duration expireThreshold) {
        this.canonicalTypesToProcess = Collections.unmodifiableSet(new HashSet<>(
                Objects.requireNonNull(initialCanonicalTypesToProcess, "initialCanonicalTypesToProcess")));
        this.documentEventsBatchSize = Objects.requireNonNull(initialDocumentEventsBatchSize, "initialDocumentEventsBatchSize");
        this.processingTimeout = Objects.requireNonNull(processingTimeout, "processingTimeout");
        this.expireThreshold = Objects.requireNonNull(expireThreshold, "expireThreshold");
    }

    /**
     * Returns a new {@code Set} with the current state of the canonical types. Updates to
     * configured canonical types will not be visible to the returned {@code Set}.
     */
    @Override
    public Set<String> getCanonicalTypesToProcess() {
        return canonicalTypesToProcess;
    }

    public MutableLightblueDocumentEventRepositoryConfig setCanonicalTypesToProcess(
            Collection<String> types) {
        Set<String> old = canonicalTypesToProcess;
        canonicalTypesToProcess = Collections.unmodifiableSet(new HashSet<>(types));

        if (!old.equals(canonicalTypesToProcess)) {
            List<String> removed = old.stream()
                    .filter(oldCanonicalType -> !canonicalTypesToProcess.contains(oldCanonicalType))
                    .collect(Collectors.toList());
            List<String> added = canonicalTypesToProcess.stream()
                    .filter(newCanonicalType -> !old.contains(newCanonicalType))
                    .collect(Collectors.toList());
            log.info("Canonical types to process updated. Removed {}. Added {}. " +
                    "Currently processing {}.", removed, added, canonicalTypesToProcess);
        }

        return this;
    }

    @Override
    public Integer getDocumentEventsBatchSize() {
        return documentEventsBatchSize;
    }

    @Override
    public Duration getDocumentEventProcessingTimeout() {
        return processingTimeout;
    }

    public MutableLightblueDocumentEventRepositoryConfig setDocumentEventProcessingTimeout(
            Duration processingTimeout) {
        Duration old = this.processingTimeout;
        this.processingTimeout = processingTimeout;
        if (!old.equals(processingTimeout)) {
            log.info("Document event processing timeout updated." +
                    " Old value was {}. New value is {}.", old, processingTimeout);
        }
        return this;
    }

    @Override
    public Duration getDocumentEventExpireThreshold() {
        return expireThreshold;
    }

    public MutableLightblueDocumentEventRepositoryConfig setDocumentEventExpireThreshold(
            Duration expireThreshold) {
        Duration old = this.expireThreshold;
        this.expireThreshold = expireThreshold;
        if (!old.equals(expireThreshold)) {
            log.info("Document event expire threshold updated." +
                    " Old value was {}. New value is {}.", old, expireThreshold);
        }
        return this;
    }

    public MutableLightblueDocumentEventRepositoryConfig setDocumentEventsBatchSize(int batchSize) {
        int old = documentEventsBatchSize;
        documentEventsBatchSize = batchSize;
        if (old != documentEventsBatchSize) {
            log.info("Document event batch size updated." +
                    " Old value was {}. New value is {}.", old, documentEventsBatchSize);
        }
        return this;
    }
}
