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

import org.esbtools.eventhandler.lightblue.model.EventStatus;
import org.esbtools.eventhandler.lightblue.model.IdentityValue;
import org.esbtools.eventhandler.lightblue.model.DocumentEventEntity;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Because there are two event schemas backing the {@link LightblueEventRepository} that share the
 * same core underlying data model, it is useful to be able to work with either event schema. This
 * interface provides a view of the data model, regardless of the schema of the event it is
 * derived from.
 *
 * @see CommonEventViewOfNotification
 * @see CommonEventViewOfDocumentEvent
 */
public interface CommonEventView {

    String entityName();

    String entityVersion();

    List<IdentityValue> entityIdentity();

    List<IdentityValue> normalizedEntityIdentity();

    Optional<Instant> occurrenceDate();

    Optional<Instant> publishDate();

    EventStatus status();

    Optional<LightblueNotification> notification();

    Optional<DocumentEventEntity> documentEvent();
}
