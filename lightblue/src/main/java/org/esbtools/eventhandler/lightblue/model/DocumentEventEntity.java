/*
 *  Copyright 2015 esbtools Contributors and/or its affiliates.
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

import io.github.alechenninger.lightblue.Identity;
import io.github.alechenninger.lightblue.MinItems;
import io.github.alechenninger.lightblue.Required;
import io.github.alechenninger.lightblue.Transient;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/**
 * Serialization-friendly "data object" for an entity in the documentEvent collection.
 */
public class DocumentEventEntity {
    private String _id;
    private String canonicalType;
    private List<KeyAndValue> parameters;
    private Status status;
    private Integer priority;
    private Instant creationDate;
    private Instant processedDate;
    private String survivedById;

    public String get_id() {
        return _id;
    }

    @Identity
    public void set_id(String _id) {
        this._id = _id;
    }

    public String getCanonicalType() {
        return canonicalType;
    }

    @Required
    public void setCanonicalType(String canonicalType) {
        this.canonicalType = canonicalType;
    }

    @Transient
    @Nullable
    public String getParameterByKey(String key) {
        for (KeyAndValue keyAndValue : parameters) {
            if (Objects.equals(key, keyAndValue.getKey())) {
                return keyAndValue.getValue();
            }
        }
        throw new NoSuchElementException(key);
    }

    public List<KeyAndValue> getParameters() {
        return parameters;
    }

    @Required
    @MinItems(1)
    public void setParameters(List<KeyAndValue> parameters) {
        this.parameters = parameters;
    }

    public Status getStatus() {
        return status;
    }

    @Required
    public void setStatus(Status status) {
        this.status = status;
    }

    public Instant getCreationDate() {
        return creationDate;
    }

    @Required
    public void setCreationDate(Instant creationDate) {
        this.creationDate = creationDate;
    }

    public Instant getProcessedDate() {
        return processedDate;
    }

    public void setProcessedDate(Instant processedDate) {
        this.processedDate = processedDate;
    }

    public String getSurvivedById() {
        return survivedById;
    }

    public void setSurvivedById(String survivedById) {
        this.survivedById = survivedById;
    }

    public Integer getPriority() {
        return priority;
    }

    @Required
    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public enum Status {
        /** New raw event */
        NEW("new"),

        /** Being processed (transient state) */
        PROCESSING("processing"),

        /** Processed */
        PROCESSED("processed"),

        /** Superseded by a duplicate event */
        SUPERSEDED("superseded"),

        /** Merged into another event */
        MERGED("merged"),

        FAILED("failed");

        private final String toString;

        Status(String toString) {
            this.toString = toString;
        }

        public String toString() {
            return toString;
        }
    }

    public static class KeyAndValue {
        private String key;
        private String value;

        public KeyAndValue(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return this.key;
        }

        @Required
        public void setKey(String key) {
            this.key = key;
        }

        public String getValue() {
            return this.value;
        }

        @Required
        public void setValue(String value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            KeyAndValue identityValue = (KeyAndValue) o;
            return Objects.equals(key, identityValue.key) &&
                    Objects.equals(value, identityValue.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }

        @Override
        public String toString() {
            return "KeyAndValue{" +
                    "key='" + key + '\'' +
                    ", value='" + value + '\'' +
                    '}';
        }
    }
}
