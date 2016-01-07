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

import org.esbtools.lightbluenotificationhook.NotificationEntity;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Supports creating many types of {@link LightblueNotification}s based on the entity name in the
 * provided {@link NotificationEntity}.
 */
public class ByEntityNameNotificationFactory implements NotificationFactory {
    private final Map<String, NotificationFactory> factories = new HashMap<>();

    /**
     * Add a factory for a specific type of notification, to be used when the notification is for
     * the provided entity name.
     *
     * @param name The entity name the {@link NotificationFactory} should be used for.
     * @param factory A {@code NotificationFactory} which creates {@link LightblueNotification}s for
     *                the specific entity identified by its name.
     *
     * @see NotificationEntity#getEntityName()
     */
    public ByEntityNameNotificationFactory addByEntityName(String name, NotificationFactory factory) {
        factories.put(name, factory);
        return this;
    }

    @Override
    public LightblueNotification getNotificationForEntity(NotificationEntity entity,
            LightblueRequester requester) {
        String type = entity.getEntityName();

        if (!factories.containsKey(type)) {
            throw new NoSuchElementException("No notification factory stored for entity type: "
                    + type);
        }

        return factories.get(type).getNotificationForEntity(entity, requester);
    }
}
