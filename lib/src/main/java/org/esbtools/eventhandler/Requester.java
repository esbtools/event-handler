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

/**
 * Abstracts making requests, presumably to some external system, where the remote calls can be
 * made lazily or asynchronously.
 *
 * <p>This allows for queuing up requests and performing them all in one batch request which reduces
 * network traffic should your backend support bulk requests like this.
 *
 * @param <T> The type of requests
 * @param <U> The type of responses
 */
public interface Requester<T, U> {
    ResponsePromise<T, U> queueRequests(T... requests);
}
