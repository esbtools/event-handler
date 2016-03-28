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

package org.esbtools.eventhandler;

public interface MessageFactory {
    // TODO(ahenning): This should probably either be an InputStream or a parameterized type of input
    // I think generic type is probably best: the input is a function of what natively gets sent to
    // the message processor route, which is entirely up to the developer. She also chooses what
    // message factory to use, and so those types should align. Generics communicate this naturally.
    Message getMessageForBody(Object body) throws Exception;
}
