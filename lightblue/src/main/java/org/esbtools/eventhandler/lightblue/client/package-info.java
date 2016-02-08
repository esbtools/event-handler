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

/**
 * Contains implementations of {@link org.esbtools.eventhandler.Requester} for bridging
 * {@link java.util.concurrent.Future}-based APIs with efficient, "batch-able" requests (see
 * {@link org.esbtools.eventhandler.lightblue.client.BulkLightblueRequester}).
 *
 * <p>Additionally, because constructing requets for
 * {@link com.redhat.lightblue.client.LightblueClient} can be verbose, this provides static factory
 * methods to construct these more readably (see
 * {@link org.esbtools.eventhandler.lightblue.client.UpdateRequests} for example).
 */
package org.esbtools.eventhandler.lightblue.client;
