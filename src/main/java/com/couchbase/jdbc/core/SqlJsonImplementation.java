package com.couchbase.jdbc.core;

import com.couchbase.json.SQLJSON;

import java.io.*;
import java.util.Map;

/**
 * Created by davec on 15-07-02.
 */
public class SqlJsonImplementation implements SQLJSON
{
    String sqlJson;
    public SqlJsonImplementation(String jsonString)
    {
        sqlJson = jsonString;
    }

    @Override
    public void free() {

    }

    @Override
    public InputStream getBinaryStream() {
        try
        {
            return new ByteArrayInputStream(sqlJson.getBytes("UTF-8"));
        }
        catch (UnsupportedEncodingException ex)
        {
            //TODO fixme
            return null;
        }
    }

    @Override
    public Reader getCharacterStream() {
        return new StringReader(sqlJson);
    }

    @Override
    public String getString() {
        return sqlJson;
    }

    @Override
    public void setString(String str) {
        sqlJson = str;

    }

    @Override
    public Object parse(Class clazz) {
        return null;
    }

    @Override
    public Map parse() {
        return null;
    }
}
