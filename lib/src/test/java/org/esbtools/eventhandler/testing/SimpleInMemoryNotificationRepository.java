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

package org.esbtools.eventhandler.testing;

import org.esbtools.eventhandler.FailedNotification;
import org.esbtools.eventhandler.Notification;
import org.esbtools.eventhandler.NotificationRepository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SimpleInMemoryNotificationRepository implements NotificationRepository {
    private final List<Notification> notifications = new ArrayList<>();
    private final List<Notification> processed = new ArrayList<>();
    private final List<FailedNotification> failed = new ArrayList<>();

    public void addNotifications(List<? extends Notification> notifications) {
        this.notifications.addAll(notifications);
    }

    public List<Notification> getProcessedNotifications() {
        return processed;
    }

    public List<FailedNotification> getFailedNotifications() {
        return failed;
    }

    @Override
    public List<? extends Notification> retrieveOldestNotificationsUpTo(int maxNotifications) throws Exception {
        maxNotifications = maxNotifications > notifications.size() ? notifications.size() : maxNotifications;
        List<Notification> retrieved = new ArrayList<>(notifications.subList(0, maxNotifications));
        notifications.removeAll(retrieved);
        return retrieved;
    }

    @Override
    public void markNotificationsProcessedOrFailed(Collection<? extends Notification> notification,
            Collection<FailedNotification> failures) throws Exception {
        processed.addAll(notification);
        failed.addAll(failures);
    }
}
