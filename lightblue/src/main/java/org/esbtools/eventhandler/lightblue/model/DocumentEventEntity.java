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

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;

/**
 * Serialization-friendly "data object" for an entity in the documentEvent collection.
 *
 * <p>Ready as in, "ready to be published." Ready events are normalized and prioritized.
 *
 * TODO Merge semantics TBD...
 *  - does discarded event get publishDate? - No I think. Publish date only if published.
 */
public class DocumentEventEntity {
    private String _id;
    private String entityName;
    private String entityVersion;
    private List<IdentityValue> identity;
    private JsonNode projection;
    // TODO: Status vs status dates vs created/updated dates
    private EventStatus status;
    private Instant creationDate;
    private Instant processedDate;
    private String survivedById;

    public String get_id() {
        return _id;
    }

    public DocumentEventEntity set_id(String _id) {
        this._id = _id;
        return this;
    }

    public String getEntityName() {
        return entityName;
    }

    public DocumentEventEntity setEntityName(String entityName) {
        this.entityName = entityName;
        return this;
    }

    public String getEntityVersion() {
        return entityVersion;
    }

    public DocumentEventEntity setEntityVersion(String entityVersion) {
        this.entityVersion = entityVersion;
        return this;
    }

    public List<IdentityValue> getIdentity() {
        return identity;
    }

    public DocumentEventEntity setIdentity(List<IdentityValue> identity) {
        this.identity = identity;
        return this;
    }

    public JsonNode getProjection() {
        return projection;
    }

    public DocumentEventEntity setProjection(JsonNode projection) {
        this.projection = projection;
        return this;
    }

    public EventStatus getStatus() {
        return status;
    }

    public DocumentEventEntity setStatus(EventStatus status) {
        this.status = status;
        return this;
    }

    public Instant getCreationDate() {
        return creationDate;
    }

    public DocumentEventEntity setCreationDate(Instant creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public Instant getProcessedDate() {
        return processedDate;
    }

    public DocumentEventEntity setProcessedDate(Instant processedDate) {
        this.processedDate = processedDate;
        return this;
    }

    public String getSurvivedById() {
        return survivedById;
    }

    public DocumentEventEntity setSurvivedById(String survivedById) {
        this.survivedById = survivedById;
        return this;
    }

}
