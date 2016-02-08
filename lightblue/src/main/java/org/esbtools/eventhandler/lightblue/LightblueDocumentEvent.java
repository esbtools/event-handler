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

import org.esbtools.eventhandler.DocumentEvent;

/**
 * Connects event implementations to the underlying {@link LightblueDocumentEventRepository} data
 * model.
 */
public interface LightblueDocumentEvent extends DocumentEvent {
    /**
     * {@inheritDoc}
     */
    @Override
    LightblueDocumentEvent merge(DocumentEvent event);

    /**
     * Identity is a set of key value pairs with the property of being equivalent to another
     * document event's identity when this document event is able to be merged with or is superseded
     * by that other document event.
     *
     * <p>We use a String representation of an identity as a resource for locking, so that one
     * thread has ownership of all events which are able to be optimized together.
     */
    Identity identity();

    /**
     * @return Entity backing this document event. Every document event should be backed by an
     * entity instance. This should refer to that one, mutable instance and should not create a new
     * one.
     */
    DocumentEventEntity wrappedDocumentEventEntity();
}
