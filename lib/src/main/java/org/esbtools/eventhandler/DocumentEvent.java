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

package org.esbtools.eventhandler;

import java.util.concurrent.Future;

/**
 * Models an event which corresponds to a publishable document message.
 */
public interface DocumentEvent {
    Future<?> lookupDocument();

    /**
     * If the provided event's entity is looked up, will it include whatever change that this event
     * was intended to share?
     *
     * <p>Superseded events can be safely ignored as long as an event superseding it is processed.
     *
     * <p>This is a function of both the identity of the entity represented in this event, and the
     * time of which the provided event may have been processed. For instance, if they both refer
     * to the same entity, and this event occurred before the provided event was processed, then
     * logically this event is superseded by the other. However, is this event occurred after the
     * other was published, the other does not supersede this event, despite them referring to the
     * same entity.
     *
     * <p>This is intended to be a quick optimization before other logic is performed.
     */
    boolean isSupersededBy(DocumentEvent event);

    /**
     * Determines if the entity looked up in this event and the provided event could be represented
     * instead as a single event which refers to data for both events.
     *
     * <p>If two events are equivalent, they cannot be merged. The result of a merge is a new event,
     * and it is expected that this new event be published instead of both merged events. Therefore,
     * an event that is equivalent to another should be determined to be
     * {@link #isSupersededBy(DocumentEvent) superseded}, marked at such, and ignored, simply
     * choosing one event over the other. A merge results in two events being ignored, and a new,
     * slightly different one to take both of their places.
     *
     * @see #merge(DocumentEvent)
     */
    boolean couldMergeWith(DocumentEvent event);

    /**
     * If possible, returns an event that represents both this event and the provided event. That
     * is, looking up the entity for the result of the merge will include all of the data that would
     * have been in both events.
     *
     * <p>Intended as an optimization that turns two data lookups with some redundant data, some
     * different, into one data lookup that would include both events' entity data.
     *
     * @return A new event which represents both {@code this} and the provided event.
     *
     * @throws UnsupportedOperationException if the two events could not be merged. Check if two
     * events can be merged first by calling {@link #couldMergeWith(DocumentEvent)}.
     */
    DocumentEvent merge(DocumentEvent event);
}
