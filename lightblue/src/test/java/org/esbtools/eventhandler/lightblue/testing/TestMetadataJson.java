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

package org.esbtools.eventhandler.lightblue.testing;

import com.redhat.lightblue.client.integration.test.LightblueExternalResource;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.parser.Extensions;
import com.redhat.lightblue.metadata.parser.JSONMetadataParser;
import com.redhat.lightblue.metadata.types.DefaultTypes;
import com.redhat.lightblue.mongo.common.MongoDataStore;
import com.redhat.lightblue.mongo.metadata.MongoDataStoreParser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.github.alechenninger.lightblue.MetadataGenerator;
import io.github.alechenninger.lightblue.javabeans.JavaBeansReflector;

public abstract class TestMetadataJson {
    private static final Extensions<JsonNode> extensions = new Extensions<JsonNode>() {{
        addDefaultExtensions();
        registerDataStoreParser("mongo", new MongoDataStoreParser<>());
    }};

    private static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);
    private static final JSONMetadataParser parser = new JSONMetadataParser(extensions, new DefaultTypes(), jsonNodeFactory);
    private static final MetadataGenerator metadataGenerator = new MetadataGenerator(new JavaBeansReflector());

    public static LightblueExternalResource.LightblueTestMethods forEntity(Class<?> entityClass) {
        EntityMetadata entityMd = metadataGenerator.generateMetadata(entityClass);
        entityMd.setDataStore(new MongoDataStore("${mongo.database}", "${mongo.datasource}", entityMd.getName()));

        return new LightblueExternalResource.LightblueTestMethods() {
            @Override
            public JsonNode[] getMetadataJsonNodes() throws Exception {
                return new JsonNode[] {parser.convert(entityMd)};
            }
        };
    }
}
