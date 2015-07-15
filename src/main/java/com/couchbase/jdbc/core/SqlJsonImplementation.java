package com.couchbase.jdbc.core;

import com.couchbase.json.SQLJSON;
import org.boon.core.reflection.Mapper;
import org.boon.core.reflection.MapperSimple;
import org.boon.json.JsonFactory;

import java.io.*;
import java.util.Map;

/**
 * Created by davec on 15-07-02.
 */
public class SqlJsonImplementation implements SQLJSON
{
    Map jsonObject;
    String sqlJson;

    public SqlJsonImplementation()
    {

    }
    public SqlJsonImplementation(Map map)
    {
        jsonObject = map;
    }

    @Override
    public void free() {

    }

    private void toJson()
    {
        synchronized (sqlJson)
        {
            if (sqlJson == null )
            {
                JsonFactory.toJson(jsonObject);
            }
        }

    }
    @Override
    public InputStream getBinaryStream()
    {
        toJson();
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
    public Reader getCharacterStream()
    {
        toJson();
        return new StringReader(sqlJson);
    }

    @Override
    public String getString() {
        toJson();
        return sqlJson;
    }

    @Override
    public void setString(String str) {
        sqlJson = str;

    }

    @Override
    public Object parse(Class clazz)
    {
        Mapper mapper = new MapperSimple();
        mapper.fromMap(jsonObject,clazz);
        return null;
    }

    @Override
    public Map parse() {
        return jsonObject;
    }
}
