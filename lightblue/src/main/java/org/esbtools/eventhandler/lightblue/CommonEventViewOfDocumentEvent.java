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

import org.esbtools.eventhandler.lightblue.model.DocumentEventEntity;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class CommonEventViewOfDocumentEvent implements CommonEventView {
    private final DocumentEventEntity documentEventEntity;

    public CommonEventViewOfDocumentEvent(DocumentEventEntity documentEventEntity) {
        this.documentEventEntity = documentEventEntity;
    }

    @Override
    public String entityName() {
        return documentEventEntity.getCanonicalType();
    }

    @Override
    public String entityVersion() {
        return documentEventEntity.getEntityVersion();
    }

    @Override
    public List<DocumentEventEntity.KeyAndValue> entityIdentity() {
        return documentEventEntity.getEntityIdentity();
    }

    @Override
    public List<DocumentEventEntity.KeyAndValue> entityIncludedFields() {
        return Collections.emptyList();
    }

    @Override
    public Optional<Instant> occurrenceDate() {
        return Optional.empty();
    }

    @Override
    public Optional<Instant> publishDate() {
        return Optional.ofNullable(documentEventEntity.getProcessedDate());
    }

    @Override
    public DocumentEventEntity.Status status() {
        return documentEventEntity.getStatus();
    }

    @Override
    public Optional<LightblueNotification> notification() {
        return Optional.empty();
    }

    @Override
    public Optional<DocumentEventEntity> documentEvent() {
        return Optional.of(documentEventEntity);
    }
}
