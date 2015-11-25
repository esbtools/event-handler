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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Serialization-friendly "data object" for an entity in the notification collection.
 *
 * <p>Notifications include just the very immediate, basic information known at the time of a CRUD
 * operation on an integrated entity on integrated fields.
 */
public class NotificationEntity {
    private String _id;
    private String entityName;
    private String entityVersion;
    private List<IdentityValue> entityIdentity = new ArrayList<>();
    private List<IdentityValue> entityCapturedFields = new ArrayList<>();
    private EventStatus status;
    private Operation operation;
    /**
     * This should probably be who triggered the notification. We only do certain things if ESB
     * itself wasn't the thing that updated the data.
     */
    private String eventSource;
    private Instant occurrenceDate;

    public enum Operation {
        INSERT, UPDATE, SYNC
    }

    public String get_id() {
        return _id;
    }

    public NotificationEntity set_id(String _id) {
        this._id = _id;
        return this;
    }

    public String getEntityName() {
        return entityName;
    }

    public NotificationEntity setEntityName(String entityName) {
        this.entityName = entityName;
        return this;
    }

    public String getEntityVersion() {
        return entityVersion;
    }

    public NotificationEntity setEntityVersion(String entityVersion) {
        this.entityVersion = entityVersion;
        return this;
    }

    public List<IdentityValue> getEntityIdentity() {
        return entityIdentity;
    }

    public NotificationEntity setEntityIdentity(List<IdentityValue> entityIdentity) {
        this.entityIdentity = entityIdentity;
        return this;
    }

    public List<IdentityValue> getEntityCapturedFields() {
        return entityCapturedFields;
    }

    public NotificationEntity setEntityCapturedFields(List<IdentityValue> entityCapturedFields) {
        this.entityCapturedFields = entityCapturedFields;
        return this;
    }

    public EventStatus getStatus() {
        return status;
    }

    public NotificationEntity setStatus(EventStatus status) {
        this.status = status;
        return this;
    }

    public NotificationEntity setSupersededBy(String supersededById) {
        return this;
    }

    public Operation getOperation() {
        return operation;
    }

    public NotificationEntity setOperation(Operation operation) {
        this.operation = operation;
        return this;
    }

    public String getEventSource() {
        return eventSource;
    }

    public NotificationEntity setEventSource(String eventSource) {
        this.eventSource = eventSource;
        return this;
    }

    public Instant getOccurrenceDate() {
        return occurrenceDate;
    }

    public NotificationEntity setOccurrenceDate(Instant occurrenceDate) {
        this.occurrenceDate = occurrenceDate;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NotificationEntity that = (NotificationEntity) o;
        return Objects.equals(_id, that._id) &&
                Objects.equals(entityName, that.entityName) &&
                Objects.equals(entityVersion, that.entityVersion) &&
                Objects.equals(entityIdentity, that.entityIdentity) &&
                Objects.equals(entityCapturedFields, that.entityCapturedFields) &&
                status == that.status &&
                operation == that.operation &&
                Objects.equals(eventSource, that.eventSource) &&
                Objects.equals(occurrenceDate, that.occurrenceDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_id, entityName, entityVersion, entityIdentity, entityCapturedFields, status, operation, eventSource, occurrenceDate);
    }

    @Override
    public String toString() {
        return "NotificationEntity{" +
                "_id='" + _id + '\'' +
                ", entityName='" + entityName + '\'' +
                ", entityVersion='" + entityVersion + '\'' +
                ", entityIdentity=" + entityIdentity +
                ", normalizedEntityIdentity=" + entityCapturedFields +
                ", status=" + status +
                ", operation=" + operation +
                ", eventSource='" + eventSource + '\'' +
                ", occurrenceDate=" + occurrenceDate +
                '}';
    }
}
