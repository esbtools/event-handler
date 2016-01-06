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

package org.esbtools.eventhandler.lightblue.testing;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.Futures;
import org.esbtools.eventhandler.DocumentEvent;
import org.esbtools.eventhandler.lightblue.LightblueDocumentEvent;
import org.esbtools.eventhandler.lightblue.LightblueRequester;
import org.esbtools.eventhandler.lightblue.model.DocumentEventEntity;

import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;

/**
 * A test document event which looks up many predetermined String values. The canonical type of the
 * document is simply "Strings".
 *
 * <p>This is useful for testing document event merges. Every Strings event is merge-able: the
 * merged event includes both victims' values.
 */
public class MultiStringDocumentEvent implements LightblueDocumentEvent {
    private final List<String> values;
    private final ZonedDateTime creationDate;
    private final DocumentEventEntity wrappedEntity;
    private final Clock clock;

    public MultiStringDocumentEvent(List<String> values, Clock clock) {
        this.values = values;
        this.clock = clock;

        creationDate = ZonedDateTime.now(clock);
        wrappedEntity = toNewDocumentEventEntity();
    }

    public List<String> values() {
        return values;
    }

    public MultiStringDocumentEvent(DocumentEventEntity wrappedEntity) {
        this.wrappedEntity = wrappedEntity;
        this.clock = Clock.systemDefaultZone();

        values = Arrays.asList(wrappedEntity.getParameterByKey("values").split("\\|"));
        creationDate = wrappedEntity.getCreationDate();
    }

    public MultiStringDocumentEvent(DocumentEventEntity wrappedEntity, LightblueRequester requester) {
        this(wrappedEntity);
    }

    @Override
    public DocumentEventEntity wrappedDocumentEventEntity() {
        return wrappedEntity;
    }

    public DocumentEventEntity toNewDocumentEventEntity() {
        DocumentEventEntity entity = new DocumentEventEntity();
        entity.setCanonicalType("MultiString");
        entity.setParameters(Arrays.asList(
                new DocumentEventEntity.KeyAndValue("values", Joiner.on('|').join(values))));
        entity.setStatus(DocumentEventEntity.Status.unprocessed);
        entity.setCreationDate(creationDate);
        entity.setPriority(50);
        return entity;
    }

    @Override
    public Future<?> lookupDocument() {
        return Futures.immediateFuture(values);
    }

    @Override
    public boolean isSupersededBy(DocumentEvent event) {
        if (!(event instanceof MultiStringDocumentEvent)) {
            return false;
        }

        MultiStringDocumentEvent other = (MultiStringDocumentEvent) event;

        if (!Objects.equals(other.values, values)) {
            return false;
        }

        DocumentEventEntity otherEntity = other.wrappedDocumentEventEntity();

        if (otherEntity.getStatus().equals(DocumentEventEntity.Status.processed) &&
                otherEntity.getProcessedDate().isBefore(creationDate)) {
            return false;
        }

        return true;
    }

    @Override
    public boolean couldMergeWith(DocumentEvent event) {
        return event instanceof MultiStringDocumentEvent;
    }

    @Override
    public LightblueDocumentEvent merge(DocumentEvent event) {
        if (!couldMergeWith(event)) {
            throw new IllegalArgumentException(event.toString());
        }

        MultiStringDocumentEvent other = (MultiStringDocumentEvent) event;

        List<String> mergedValues = new ArrayList<>();
        mergedValues.addAll(other.values);
        mergedValues.addAll(this.values);

        return new MultiStringDocumentEvent(mergedValues, clock);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MultiStringDocumentEvent that = (MultiStringDocumentEvent) o;
        return Objects.equals(values, that.values) &&
                Objects.equals(creationDate, that.creationDate) &&
                Objects.equals(wrappedEntity, that.wrappedEntity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values, creationDate, wrappedEntity);
    }

    @Override
    public String toString() {
        return "StringsDocumentEvent{" +
                "values=" + values +
                ", creationDate=" + creationDate +
                ", wrappedEntity=" + wrappedEntity +
                '}';
    }
}
