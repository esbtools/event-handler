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

package org.esbtools.eventhandler.lightblue;

import com.google.common.util.concurrent.Futures;
import org.esbtools.eventhandler.DocumentEvent;
import org.esbtools.eventhandler.lightblue.model.DocumentEventEntity;

import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.Future;

/**
 * A test document event which looks up a predetermined String value. The canonical type of the
 * document is simply "String".
 */
public final class StringDocumentEvent implements LightblueDocumentEvent {
    private final String value;
    private final ZonedDateTime creationDate;
    private final DocumentEventEntity wrappedEntity;

    public StringDocumentEvent(String value, Clock clock) {
        this.value = value;

        creationDate = ZonedDateTime.now(clock);

        DocumentEventEntity entity = new DocumentEventEntity();
        entity.setCanonicalType("String");
        entity.setParameters(Arrays.asList(new DocumentEventEntity.KeyAndValue("value", this.value)));
        entity.setStatus(DocumentEventEntity.Status.unprocessed);
        entity.setPriority(50);
        entity.setCreationDate(creationDate);

        wrappedEntity = entity;
    }

    public StringDocumentEvent(DocumentEventEntity wrappedEntity) {
        this.wrappedEntity = wrappedEntity;

        value = wrappedEntity.getParameterByKey("value");
        creationDate = wrappedEntity.getCreationDate();
    }

    public StringDocumentEvent(DocumentEventEntity wrappedEntity, LightblueRequester requester) {
        this(wrappedEntity);
    }

    @Override
    public DocumentEventEntity wrappedDocumentEventEntity() {
        return wrappedEntity;
    }

    @Override
    public Future<?> lookupDocument() {
        return Futures.immediateFuture(value);
    }

    @Override
    public boolean isSupersededBy(DocumentEvent event) {
        if (!(event instanceof StringDocumentEvent)) {
            return false;
        }

        StringDocumentEvent other = (StringDocumentEvent) event;

        if (!Objects.equals(other.value, value)) {
            return false;
        }

        DocumentEventEntity otherEntity = other.wrappedDocumentEventEntity();

        if (Objects.equals(otherEntity.getStatus(), DocumentEventEntity.Status.processed) &&
                otherEntity.getProcessedDate().isBefore(creationDate)) {
            return false;
        }

        return true;
    }

    @Override
    public boolean couldMergeWith(DocumentEvent event) {
        return false;
    }

    @Override
    public DocumentEvent merge(DocumentEvent event) {
        throw new UnsupportedOperationException("Can't do that");
    }

    @Override
    public String toString() {
        return "StringDocumentEvent{" +
                "value='" + value + '\'' +
                ", creationDate=" + creationDate +
                ", wrappedEntity=" + wrappedEntity +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StringDocumentEvent that = (StringDocumentEvent) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(creationDate, that.creationDate) &&
                Objects.equals(wrappedEntity, that.wrappedEntity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, creationDate, wrappedEntity);
    }
}
