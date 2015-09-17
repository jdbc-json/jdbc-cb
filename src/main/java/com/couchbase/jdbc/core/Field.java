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

package com.couchbase.jdbc.core;

import com.couchbase.jdbc.util.JSONTypes;

public class Field
{
    String name;
    String type;

    public Field(String name, String type )
    {
        this.name = name;
        this.type = type;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        if (type.equalsIgnoreCase("true") || type.equalsIgnoreCase("false") )
        {
            this.type="boolean";
        }
        else if (type.equalsIgnoreCase("null"))
        {
            this.type="unknown";
        }
        else
        {
            this.type = type;
        }
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }


    public int getSqlType()
    {
        return JSONTypes.jdbcTypes.get(type);
    }
}
