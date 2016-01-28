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

import java.util.Collection;
import java.util.List;

public interface NotificationRepository {

    /**
     * Retrieves the oldest {@code maxNotifications} {@link Notification notifications}, oldest first.
     *
     * <p>Once retrieved, a notification should not be retrieved again, atomically. That is, many
     * threads looking up notifications at the same time should all et a unique non-overlapping sample
     * of the oldest notifications. Subsequent calls should always return a unique set.
     */
    List<? extends Notification> retrieveOldestNotificationsUpTo(int maxNotifications) throws Exception;

    void markNotificationsProcessedOrFailed(Collection<? extends Notification> notification,
            Collection<FailedNotification> failures) throws Exception;
}
