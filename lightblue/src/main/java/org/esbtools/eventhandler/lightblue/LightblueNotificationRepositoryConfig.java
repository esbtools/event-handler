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

import java.time.Duration;
import java.util.Set;

public interface LightblueNotificationRepositoryConfig {
    /**
     * Governs whether or not notifications are processed based on their associated entity's name.
     */
    Set<String> getEntityNamesToProcess();

    /**
     * How long can a notification remain unchanged after marked as processing before we allow it
     * to be retrieved again for reprocessing?
     */
    Duration getNotificationProcessingTimeout();

    /**
     * How long before a notification is available for retrieval do we drop the event and let it be
     * reprocessed?
     *
     * <p>In other words, this governs when we stop processing a notification in flight because
     * we're too near when another retrieval may see it is past its
     * {@link #getNotificationProcessingTimeout()} and retrieve it for reprocessing.
     *
     * <p>N.B. The existence of this configuration is a function of our current transaction scheme.
     * This could go away, for instance, if we either atomically updated a notification's processing
     * timestamp before adding its document events. Other alternative schemes are possible.
     */
    Duration getNotificationExpireThreshold();
}
