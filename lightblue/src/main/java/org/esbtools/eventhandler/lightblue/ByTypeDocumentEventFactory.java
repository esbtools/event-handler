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

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Supports creating many types of {@link LightblueDocumentEvent}s based on the canonical message
 * type in the provided {@link DocumentEventEntity}.
 */
public class ByTypeDocumentEventFactory implements DocumentEventFactory {
    private final Map<String, DocumentEventFactory> factories = new HashMap<>();

    /**
     * Add a factory for document events of a specific canonical type. When
     * {@link #getDocumentEventForEntity(DocumentEventEntity, LightblueRequester)} is passed a
     * {@link DocumentEventEntity} with this canonical type, this factory will be used to create a
     * {@link LightblueDocumentEvent} for it.
     *
     * <p>Only one factory per type is used, and subsequent calls with the same type will overwrite
     * the previously assigned factory.
     *
     * @param type The canonical type to use this factory for.
     * @param factory A {@code DocumentEventFactory} which creates {@link LightblueDocumentEvent}s
     *                for the specific canonical type.
     *
     * @see DocumentEventEntity#getCanonicalType()
     */
    public ByTypeDocumentEventFactory addType(String type, DocumentEventFactory factory) {
        factories.put(type, factory);
        return this;
    }

    @Override
    public LightblueDocumentEvent getDocumentEventForEntity(DocumentEventEntity entity,
            LightblueRequester requester) {
        String type = entity.getCanonicalType();

        if (!factories.containsKey(type)) {
            throw new NoSuchElementException("No document event factory stored for entity type: "
                    + type);
        }

        return factories.get(type).getDocumentEventForEntity(entity, requester);
    }
}
