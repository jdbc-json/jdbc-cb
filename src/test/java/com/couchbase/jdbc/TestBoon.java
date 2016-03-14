/*
 * //  Copyright (c) 2015 Couchbase, Inc.
 * //  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * //  except in compliance with the License. You may obtain a copy of the License at
 * //    http://www.apache.org/licenses/LICENSE-2.0
 * //  Unless required by applicable law or agreed to in writing, software distributed under the
 * //  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * //  either express or implied. See the License for the specific language governing permissions
 * //  and limitations under the License.
 */

package com.couchbase.jdbc;

import junit.framework.TestCase;
import org.boon.core.value.ValueList;
import org.boon.core.value.ValueMap;
import org.boon.json.JsonSerializer;
import org.boon.json.JsonSerializerFactory;
import org.boon.json.JsonSlurper;
import org.junit.Test;

import java.io.File;
import java.net.URL;

/**
 * Created by davec on 2015-05-18.
 */
public class TestBoon extends TestCase
{
    @Test
    public void testFlatten1()
    {
        ClassLoader classLoader = getClass().getClassLoader();
        URL url = classLoader.getResource("example1.json");
        String path = url.getFile();
        File file = new File(path);

        ValueMap jsonObject = (ValueMap)new JsonSlurper().parse(file);

        ValueList results  = (ValueList)jsonObject.get("results");

        JsonSerializer jsonSerializer = new JsonSerializerFactory().outputType().create();


        System.out.println(jsonSerializer.serialize(jsonObject));

    }
}
