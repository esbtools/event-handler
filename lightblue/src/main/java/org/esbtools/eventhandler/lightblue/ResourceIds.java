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

public abstract class ResourceIds {
    public static String[] forNotificationsForEntities(String[] entities) {
        String[] resourceIds = new String[entities.length];

        for (int i = 0; i < entities.length; i++) {
            resourceIds[i] = "notification_" + entities[i];
        }

        return resourceIds;
    }

    public static String[] forDocumentEventsForTypes(String[] types) {
        String[] resourceIds = new String[types.length];

        for (int i = 0; i < types.length; i++) {
            resourceIds[i] = "documentEvent_" + types[i];
        }

        return resourceIds;
    }
}
