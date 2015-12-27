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

import com.redhat.lightblue.client.util.ClientConstants;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.github.alechenninger.lightblue.EntityName;
import io.github.alechenninger.lightblue.Identity;
import io.github.alechenninger.lightblue.MinItems;
import io.github.alechenninger.lightblue.Required;
import io.github.alechenninger.lightblue.Transient;
import io.github.alechenninger.lightblue.Version;

import javax.annotation.Nullable;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * Serialization-friendly "data object" for an entity in the documentEvent collection.
 */
@EntityName(DocumentEventEntity.ENTITY_NAME)
@Version(value = DocumentEventEntity.VERSION, changelog = "Initial prototype")
public class DocumentEventEntity {
    public static final String ENTITY_NAME = "documentEvent";
    public static final String VERSION = "0.1.0";

    private String _id;
    private String canonicalType;
    private List<KeyAndValue> parameters;
    private Status status;
    private Integer priority;
    private ZonedDateTime creationDate;
    private ZonedDateTime processedDate;
    private String survivedById;
    private Set<String> survivorOfIds;

    private static final String LIGHTBLUE_DATE_FORMAT = ClientConstants.LIGHTBLUE_DATE_FORMAT_STR;

    public static DocumentEventEntity newlyCreated(String canonicalType, int priority,
            ZonedDateTime creationDate, KeyAndValue... parameters) {
        DocumentEventEntity entity = new DocumentEventEntity();
        entity.setStatus(Status.unprocessed);
        entity.setCreationDate(creationDate);
        entity.setCanonicalType(canonicalType);
        entity.setPriority(priority);
        entity.setParameters(Arrays.asList(parameters));
        return entity;
    }

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

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = LIGHTBLUE_DATE_FORMAT)
    public ZonedDateTime getCreationDate() {
        return creationDate;
    }

    @Required
    public void setCreationDate(ZonedDateTime creationDate) {
        this.creationDate = creationDate;
    }

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = LIGHTBLUE_DATE_FORMAT)
    public ZonedDateTime getProcessedDate() {
        return processedDate;
    }

    public void setProcessedDate(ZonedDateTime processedDate) {
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

    public Set<String> getSurvivorOfIds() {
        return survivorOfIds;
    }

    public void setSurvivorOfIds(Set<String> survivorOfIds) {
        this.survivorOfIds = survivorOfIds;
    }

    public void addSurvivorOfIds(@Nullable String... ids) {
        if (ids == null) return;

        if (survivorOfIds == null) {
            survivorOfIds = new HashSet<>();
        }
        Collections.addAll(survivorOfIds, ids);
    }

    public void addSurvivorOfIds(@Nullable  Collection<String> ids) {
        if (ids == null) return;

        if (survivorOfIds == null) {
            survivorOfIds = new HashSet<>();
        }

        survivorOfIds.addAll(ids);
    }

    @Override
    public String toString() {
        return "DocumentEventEntity{" +
                "_id='" + _id + '\'' +
                ", canonicalType='" + canonicalType + '\'' +
                ", parameters=" + parameters +
                ", status=" + status +
                ", priority=" + priority +
                ", creationDate=" + creationDate +
                ", processedDate=" + processedDate +
                ", survivedById='" + survivedById + '\'' +
                ", survivorOfIds=" + survivorOfIds +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DocumentEventEntity that = (DocumentEventEntity) o;
        return Objects.equals(_id, that._id) &&
                Objects.equals(canonicalType, that.canonicalType) &&
                Objects.equals(parameters, that.parameters) &&
                status == that.status &&
                Objects.equals(priority, that.priority) &&
                Objects.equals(creationDate, that.creationDate) &&
                Objects.equals(processedDate, that.processedDate) &&
                Objects.equals(survivedById, that.survivedById) &&
                Objects.equals(survivorOfIds, that.survivorOfIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_id, canonicalType, parameters, status, priority, creationDate,
                processedDate, survivedById, survivorOfIds);
    }

    public enum Status {
        /** Initial state */
        unprocessed,

        /** Being processed (transient state) */
        processing,

        /** Indicates this document event's associated document has successfully been published */
        processed,

        /** Superseded by a duplicate event */
        superseded,

        /** Merged into another event */
        merged,

        /** Something went wrong trying to construct a document for this event */
        failed;
    }

    public static class KeyAndValue {
        private String key;
        private String value;

        public KeyAndValue() {}

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
