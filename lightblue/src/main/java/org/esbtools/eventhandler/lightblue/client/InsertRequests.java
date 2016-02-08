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

package org.esbtools.eventhandler.lightblue.client;

import org.esbtools.eventhandler.lightblue.DocumentEventEntity;

import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.request.data.DataInsertRequest;

import java.util.Collection;

public abstract class InsertRequests {
    public static DataInsertRequest documentEventsReturningOnlyIds(
            DocumentEventEntity... documentEventEntities) {
        DataInsertRequest insert = new DataInsertRequest(
                DocumentEventEntity.ENTITY_NAME,
                DocumentEventEntity.VERSION);
        insert.create(documentEventEntities);
        insert.returns(Projection.includeField("_id"));
        return insert;
    }

    public static DataInsertRequest documentEventsReturningOnlyIds(
            Collection<DocumentEventEntity> documentEventEntities) {
        DataInsertRequest insert = new DataInsertRequest(
                DocumentEventEntity.ENTITY_NAME,
                DocumentEventEntity.VERSION);
        insert.create(documentEventEntities);
        insert.returns(Projection.includeField("_id"));
        return insert;
    }
}
