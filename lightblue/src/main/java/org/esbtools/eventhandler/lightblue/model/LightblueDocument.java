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

package org.esbtools.eventhandler.lightblue.model;

import java.util.Objects;

public final class LightblueDocument {
    private final String canonicalEntity;
    private final String document;

    public LightblueDocument(String canonicalEntity, String document) {
        this.canonicalEntity = canonicalEntity;
        this.document = document;
    }

    public String getCanonicalEntity() {
        return canonicalEntity;
    }

    public String getDocument() {
        return document;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LightblueDocument that = (LightblueDocument) o;
        return Objects.equals(canonicalEntity, that.canonicalEntity) &&
                Objects.equals(document, that.document);
    }

    @Override
    public int hashCode() {
        return Objects.hash(canonicalEntity, document);
    }

    @Override
    public String toString() {
        return "LightblueMessage{" +
                "canonicalEntity='" + canonicalEntity + '\'' +
                ", document='" + document + '\'' +
                '}';
    }
}
