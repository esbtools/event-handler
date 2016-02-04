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

import javax.annotation.concurrent.ThreadSafe;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@ThreadSafe
public class MutableLightblueDocumentEventRepositoryConfig implements LightblueDocumentEventRepositoryConfig {
    private final Set<String> canonicalTypesToProcess = Collections.synchronizedSet(new HashSet<>());
    private final AtomicInteger documentEventsBatchSize = new AtomicInteger(0);
    private volatile Duration processingTimeout = Duration.ZERO;
    private volatile Duration expireThreshold = Duration.ZERO;

    /**
     * Uses empty default values, which will configure a repository to never retrieve anything.
     */
    public MutableLightblueDocumentEventRepositoryConfig() {}

    /**
     * Uses provided as initial values.
     */
    public MutableLightblueDocumentEventRepositoryConfig(
            Collection<String> initialCanonicalTypesToProcess,
            int initialDocumentEventsBatchSize, Duration processingTimeout,
            Duration expireThreshold) {
        canonicalTypesToProcess.addAll(initialCanonicalTypesToProcess);
        documentEventsBatchSize.set(initialDocumentEventsBatchSize);
        this.processingTimeout = Objects.requireNonNull(processingTimeout, "processingTimeout");
        this.expireThreshold = Objects.requireNonNull(expireThreshold, "expireThreshold");
    }

    /**
     * Returns a new {@code Set} with the current state of the canonical types. Updates to
     * configured canonical types will not be visible to the returned {@code Set}.
     */
    @Override
    public Set<String> getCanonicalTypesToProcess() {
        return new HashSet<>(canonicalTypesToProcess);
    }

    public MutableLightblueDocumentEventRepositoryConfig setCanonicalTypesToProcess(Collection<String> types) {
        if (!canonicalTypesToProcess.equals(types)) {
            synchronized (canonicalTypesToProcess) {
                canonicalTypesToProcess.clear();
                canonicalTypesToProcess.addAll(types);
            }
        }

        return this;
    }

    @Override
    public Integer getDocumentEventsBatchSize() {
        return documentEventsBatchSize.get();
    }

    @Override
    public Duration getDocumentEventProcessingTimeout() {
        return processingTimeout;
    }

    public MutableLightblueDocumentEventRepositoryConfig setDocumentEventProcessingTimeout(
            Duration processingTimeout) {
        this.processingTimeout = processingTimeout;
        return this;
    }

    @Override
    public Duration getDocumentEventExpireThreshold() {
        return expireThreshold;
    }

    public MutableLightblueDocumentEventRepositoryConfig setDocumentEventExpireThreshold(
            Duration expireThreshold) {
        this.expireThreshold = expireThreshold;
        return this;
    }

    public MutableLightblueDocumentEventRepositoryConfig setDocumentEventsBatchSize(int batchSize) {
        documentEventsBatchSize.set(batchSize);
        return this;
    }
}
