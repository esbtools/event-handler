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

/**
 * Abstracts a transactional backing store and staging area for {@link Notification notifications}.
 *
 * <p>A repository is responsible for handling CRUD and data parsing operations around these
 * objects, in whatever scheme necessary.
 *
 * <p>Production implementations are expected to be thread safe, even across a network.
 */
public interface NotificationRepository {

    /**
     * Retrieves the oldest {@code maxNotifications} {@link Notification notifications}, oldest
     * first.
     *
     * <p>Once retrieved, a notification should not be retrieved again, atomically. That is, many
     * threads looking up notifications at the same time should all et a unique non-overlapping
     * sample of the oldest notifications. Subsequent calls should always return a unique set.
     *
     * <p>Retrieved notifications begin a transaction with those notifications. Calling
     * {@link #markNotificationsProcessedOrFailed(Collection, Collection)} ends this transaction
     * on the provided notifications. This transaction may end for other reasons, such as a
     * distributed lock failure or timeout, which would cause subsequent calls to retrieve these
     * same notifications again. To determine if a transaction is still active around this, before
     * notifications' document events are added, {@link #ensureTransactionActive(Notification)}
     * should be called in order to determine if that notification's transaction is lost or has
     * otherwise ended prematurely.
     */
    List<? extends Notification> retrieveOldestNotificationsUpTo(int maxNotifications) throws Exception;

    /**
     * Throws a descriptive exception if the provided {@code notification} is not in an active
     * transaction, or if the current state of its transaction is unknown.
     *
     * <p>Transactions are started when an event is retrieved from
     * {@link #retrieveOldestNotificationsUpTo(int)}.
     *
     * <p>Transactions can end before published or failure confirmation for a variety of reasons,
     * such as network failure or timeout, depending on the implementation.
     *
     * @throws Exception if the event does not have an active transaction, and therefore is
     * available for processing from {@link #retrieveOldestNotificationsUpTo(int)}.
     */
    // TODO: Consider moving this to Notification API
    void ensureTransactionActive(Notification notification) throws Exception;

    void markNotificationsProcessedOrFailed(Collection<? extends Notification> notification,
            Collection<FailedNotification> failures) throws Exception;
}
