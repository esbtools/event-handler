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

import org.esbtools.eventhandler.DocumentEvent;

import com.google.common.util.concurrent.Futures;

import java.util.concurrent.Future;

public class UnparseableDocumentEvent implements LightblueDocumentEvent {
    private final Exception exception;
    private final DocumentEventEntity entity;

    public UnparseableDocumentEvent(Exception exception, DocumentEventEntity entity) {
        this.exception = exception;
        this.entity = entity;
    }

    @Override
    public Future<?> lookupDocument() {
        return Futures.immediateFailedFuture(exception);
    }

    @Override
    public boolean isSupersededBy(DocumentEvent event) {
        return false;
    }

    @Override
    public boolean couldMergeWith(DocumentEvent event) {
        return false;
    }

    @Override
    public LightblueDocumentEvent merge(DocumentEvent event) {
        throw new UnsupportedOperationException("Cannot merge with failed event!");
    }

    @Override
    public Identity identity() {
        return new TypeAndValueIdentity(UnparseableDocumentEvent.class, entity.get_id());
    }

    @Override
    public DocumentEventEntity wrappedDocumentEventEntity() {
        return entity;
    }
}
