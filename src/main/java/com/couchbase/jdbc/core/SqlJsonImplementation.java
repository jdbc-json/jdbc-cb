package com.couchbase.jdbc.core;

import com.couchbase.json.SQLJSON;
import org.boon.core.reflection.Mapper;
import org.boon.core.reflection.MapperSimple;
import org.boon.json.JsonFactory;

import java.io.*;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by davec on 15-07-02.
 */
public class SqlJsonImplementation implements SQLJSON
{
    Field field;
    Object jsonObject;
    String sqlJson;
    boolean isNull = false;


    final static private int JSON_NUMBER = 0;
    final static private int JSON_STRING = 1;
    final static private int JSON_BOOLEAN = 2;
    final static private int JSON_ARRAY = 3;
    final static private int JSON_MAP = 4;
    final static private int JSON_OBJECT = 5;
    final static private int JSON_NULL = 6;

    static private Map <String, Integer> jsonTypes = new HashMap<String, Integer>();

    static {
        jsonTypes.put("number",JSON_NUMBER);
        jsonTypes.put("string",JSON_STRING);
        jsonTypes.put("boolean",JSON_BOOLEAN);
        jsonTypes.put("array",JSON_ARRAY);
        jsonTypes.put("map",JSON_MAP);
        jsonTypes.put("object",JSON_OBJECT);
        jsonTypes.put("null",JSON_NULL);
    }

    public SqlJsonImplementation()
    {

    }
    public SqlJsonImplementation(Object jsonObject, Field field )
    {
        this.jsonObject = jsonObject;
        this.field = field;
    }


    @Override
    public void free() {

    }

    private synchronized void toJson()
    {
        if (sqlJson == null )
        {
            sqlJson = JsonFactory.toJson(jsonObject);
        }

    }
    @Override
    public InputStream getBinaryStream() throws SQLException
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
    public Reader getCharacterStream()   throws SQLException
    {
        toJson();
        return new StringReader(sqlJson);
    }

    @Override
    public String getString() throws SQLException
    {
        int type = jsonTypes.get(field.getType());
        switch (type)
        {
            case JSON_ARRAY:
            case JSON_OBJECT:
            case JSON_MAP:
                toJson();
                return sqlJson;
            case JSON_NULL:
                return null;
            case JSON_STRING:
                return (String)jsonObject;
            case JSON_BOOLEAN:
                return Boolean.toString((boolean)jsonObject);
            case JSON_NUMBER:
                return Long.toString((long)jsonObject);
            default:
                return "";
        }

    }

    @Override
    public void setString(String str) throws SQLException
    {
        sqlJson = str;

    }

    public void setBinaryStream() throws SQLException
    {

    }

    public void setCharacterStream(Reader stream) throws SQLException
    {

    }

    public boolean getBoolean() throws SQLException
    {
        int type = jsonTypes.get(field.getType());

        switch (type)
        {
            case JSON_BOOLEAN:
                return (boolean) jsonObject;
            case JSON_NUMBER:
                Number number = (Number)jsonObject;
                return number != (Number)0;
            case JSON_STRING:
                String string = (String) jsonObject;
                return !string.isEmpty();
            case JSON_MAP:
            case JSON_OBJECT:
                Map map = (Map) jsonObject;
                return !map.isEmpty();
            case JSON_ARRAY:
                List list = (List)jsonObject;
                return !list.isEmpty();

            default:
                return false;
        }

    }

    public void setBoolean(boolean val) throws SQLException
    {

    }

    public byte getByte() throws SQLException
    {
        if (jsonObject == null )
        {
            isNull = true;
            return 0;
        }
        else if (jsonObject instanceof Integer)
            return ((Integer)jsonObject).byteValue();
        else if (jsonObject instanceof Long)
            return ((Long)jsonObject).byteValue();
        else  if (jsonObject instanceof Byte)
            return ((Byte)jsonObject).byteValue();
        //todo this needs more options
        else return 0;
    }

    public void setByte(byte val) throws SQLException
    {
        field = new Field(null,"number" );
        jsonObject = (long)val;
    }

    public short getShort() throws SQLException
    {
        return 0;
    }

    public void setShort(short val) throws SQLException
    {

    }

    public short getInt() throws SQLException
    {
        return 0;
    }

    public void setInt(int val) throws SQLException
    {

    }

    public long getLong() throws SQLException
    {
        return 0;
    }

    public void setLong(long val) throws SQLException
    {

    }

    public BigDecimal getBigDecimal() throws SQLException
    {
        return null;
    }

    public void setBigDecimal(BigDecimal val) throws SQLException
    {

    }

    public Map getMap() throws SQLException
    {
        return null;
    }

    public void setMap(Map map) throws SQLException
    {

    }

    public Object getObject() throws SQLException
    {
        return null;
    }

    public void setObject(Object val) throws SQLException
    {

    }

    public Types getJDBCType()
    {
        return null;
    }

    @Override
    public Object parse(Class clazz)
    {
        Mapper mapper = new MapperSimple();
        mapper.fromMap((Map)jsonObject,clazz);
        return null;
    }

    @Override
    public Map parse()
    {



        if (field.getType().compareTo("json") == 0
                //if the signature is *-> * then guess using {
                || field.getType().startsWith("{"))
        {
            return (Map)jsonObject;
        }
        else return null;
    }
}
