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
 * Optional interface for implementing some abstraction around asynchronous retrieval of results for
 * given requests.
 *
 * <p>May be useful as an entry point for {@link java.util.concurrent.Future}-based APIs such as
 * {@link Notification#toDocumentEvents()} and {@link DocumentEvent#lookupDocument()}.
 *
 * <p>The only contract of a requester is that it perform the provided requests and call the right
 * {@link FutureTransform}s with the responses for the associated requests, and capture their
 * results in {@code Future}s, <em>at some point in time in the future</em>. This point of time may
 * be immediately in the same thread, lazily in the same thread, after a remote call in another
 * thread, etc. Details are up to implementation.
 *
 * <p><em>TODO(ahenning,khowell): Investigate moving away from Future-based APIs to something which
 * addresses batching more explicitly. Batching is a core concern of many of the APIs since they
 * intentionally work with Collections in their interface, and is an almost universally applicable
 * way to increase efficiency. Additionally, implementations which do not require or want batching
 * can effectively work with a batch size of "1." For those that do benefit from batching, an API
 * designed around this domain should be more straightforward to implement.</em>
 *
 * @param <T> The type of requests
 * @param <U> The type of responses
 */
public interface Requester<T, U> {
    TransformableFuture<? extends Responses<T, U>> request(T... requests);
}
