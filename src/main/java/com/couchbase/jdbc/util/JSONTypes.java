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

package com.couchbase.jdbc.util;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by davec on 2015-09-10.
 */
public class JSONTypes 
{
    final static public int JSON_NUMBER = 0;
    final static public int JSON_STRING = 1;
    final static public int JSON_BOOLEAN = 2;
    final static public int JSON_ARRAY = 3;
    final static public int JSON_MAP = 4;
    final static public int JSON_OBJECT = 5;
    final static public int JSON_NULL = 6;

    static public Map<String, Integer> jsonTypes = new HashMap<String, Integer>();

    static {
        jsonTypes.put("number",JSON_NUMBER);
        jsonTypes.put("string",JSON_STRING);
        jsonTypes.put("boolean",JSON_BOOLEAN);
        jsonTypes.put("array",JSON_ARRAY);
        jsonTypes.put("map",JSON_MAP);
        jsonTypes.put("object",JSON_OBJECT);
        jsonTypes.put("json",JSON_OBJECT);
        jsonTypes.put("null",JSON_NULL);
    }

    static public Map <String, Integer> jdbcTypes = new HashMap<String, Integer>();

    static {
        jdbcTypes.put("number", Types.NUMERIC);
        jdbcTypes.put("string", Types.VARCHAR);
        jdbcTypes.put("boolean", Types.BOOLEAN);
        jdbcTypes.put("array", Types.ARRAY);
        jdbcTypes.put("map", Types.JAVA_OBJECT); //??
        jdbcTypes.put("object", Types.JAVA_OBJECT);
        jdbcTypes.put("json", Types.JAVA_OBJECT);
        jdbcTypes.put("null", Types.NULL);

    }
}
