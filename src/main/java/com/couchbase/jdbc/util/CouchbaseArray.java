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

import javax.json.JsonArray;
import javax.json.JsonValue;
import java.sql.*;
import java.util.Map;

/**
 * Created by davec on 2015-03-06.
 */
public class CouchbaseArray implements Array
{
    JsonArray jsonArray;
    JsonValue.ValueType jsonType;

    public CouchbaseArray( Object jsonArray )
    {}
    public CouchbaseArray( JsonArray jsonArray )
    {
        this.jsonArray = jsonArray;
    }
    @Override
    public String getBaseTypeName() throws SQLException
    {

        return jsonArray.get(0).getValueType().toString();
    }

    @Override
    public int getBaseType() throws SQLException
    {
        jsonType = jsonArray.get(0).getValueType();
        switch (jsonType)
        {
            case ARRAY:     return Types.ARRAY;
            case NUMBER:    return Types.NUMERIC;
            case STRING:    return Types.VARCHAR;
            case NULL:      return Types.NULL;
            case OBJECT:    return Types.JAVA_OBJECT;
            default:        return Types.OTHER;
        }

    }

    @Override
    public Object getArray() throws SQLException
    {
        return jsonArray;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException
    {
        return null;
    }

    @Override
    public Object getArray(long index, int count) throws SQLException
    {
        return null;
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException
    {
        return null;
    }

    @Override
    public ResultSet getResultSet() throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void free() throws SQLException
    {

    }
}
