package com.couchbase.jdbc.core;

import com.couchbase.jdbc.util.TimestampUtils;
import com.couchbase.json.SQLJSON;
import org.boon.core.reflection.Mapper;
import org.boon.core.reflection.MapperSimple;
import org.boon.json.JsonFactory;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;
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
    TimestampUtils timestampUtils = new TimestampUtils();


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
        jsonTypes.put("json",JSON_OBJECT);
        jsonTypes.put("null",JSON_NULL);
    }

    static private Map <String, Integer> jdbcTypes = new HashMap<String, Integer>();

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
                if ( sqlJson != null ) return sqlJson;
                if ( jsonObject != null ) return (String) jsonObject;
                isNull = true;
                return null;

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
        field = new Field(null,"string" );

        // if it's null set the flag
        if (str == null ) isNull = true;

        // either way set the value to null
        jsonObject = str;

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
        field = new Field(null,"boolean" );
        jsonObject = (boolean)val;
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
        else  if (jsonObject instanceof Short)
            return ((Short)jsonObject).byteValue();
        else  if (jsonObject instanceof Byte)
            return ((Byte)jsonObject).byteValue();
        else if (jsonObject instanceof Double)
            return ((Double)jsonObject).byteValue();

            //todo this needs more options
        else if (!(jsonObject instanceof Number))
            throw new SQLException( "value " +jsonObject+ " not a number");
        return 0;
    }

    public void setByte(byte val) throws SQLException
    {
        field = new Field(null,"number" );
        jsonObject = (long)val;
    }

    public short getShort() throws SQLException
    {
        if (jsonObject == null )
        {
            isNull = true;
            return 0;
        }
        else if (jsonObject instanceof Integer)
            return ((Integer)jsonObject).shortValue();
        else if (jsonObject instanceof Long)
            return ((Long)jsonObject).shortValue();
        else  if (jsonObject instanceof Short)
            return ((Short)jsonObject).shortValue();
        else  if (jsonObject instanceof Byte)
            return ((Byte)jsonObject).shortValue();
        else if (jsonObject instanceof Double)
            return ((Double)jsonObject).shortValue();

        else if (!(jsonObject instanceof Number))
            throw new SQLException( "value " +jsonObject+ " not a number");
        return 0;
    }

    public void setShort(short val) throws SQLException
    {

        field = new Field(null,"number" );
        jsonObject = (long)val;

    }

    public int getInt() throws SQLException
    {
        if (jsonObject == null )
        {
            isNull = true;
            return 0;
        }
        else if (jsonObject instanceof Integer)
            return ((Integer)jsonObject).intValue();
        else if (jsonObject instanceof Long)
            return ((Long)jsonObject).intValue();
        else  if (jsonObject instanceof Short)
            return ((Short)jsonObject).intValue();
        else  if (jsonObject instanceof Byte)
            return ((Byte)jsonObject).intValue();
        else if (jsonObject instanceof Double)
            return ((Double)jsonObject).intValue();
        else if (!(jsonObject instanceof Number))
            throw new SQLException( "value " +jsonObject+ " not a number");
        return 0;

    }

    public void setInt(int val) throws SQLException
    {
        field = new Field(null,"number" );
        jsonObject = (long)val;

    }

    public long getLong() throws SQLException
    {
        if (jsonObject == null )
        {
            isNull = true;
            return 0;
        }
        else if (jsonObject instanceof Integer)
            return ((Integer)jsonObject).longValue();
        else if (jsonObject instanceof Long)
            return ((Long)jsonObject).longValue();
        else  if (jsonObject instanceof Short)
            return ((Short)jsonObject).longValue();
        else  if (jsonObject instanceof Byte)
            return ((Byte)jsonObject).longValue();
        else if (jsonObject instanceof Double)
            return ((Double)jsonObject).longValue();
        else if (!(jsonObject instanceof Number))
            throw new SQLException( "value " +jsonObject+ " not a number");
        return 0;
    }

    public void setLong(long val) throws SQLException
    {
        field = new Field(null,"number" );
        jsonObject = val;

    }

    public BigDecimal getBigDecimal() throws SQLException
    {
        if (jsonObject == null )
        {
            isNull = true;
            return null;
        }
        else if (jsonObject instanceof Integer)
            return new BigDecimal((Integer)jsonObject);
        else if (jsonObject instanceof Long)
            return new BigDecimal((Long)jsonObject);
        else  if (jsonObject instanceof Short)
            return new BigDecimal((Short)jsonObject);
        else  if (jsonObject instanceof Byte)
            return new BigDecimal((Byte)jsonObject);
        else if (jsonObject instanceof Double)
            return BigDecimal.valueOf((Double) jsonObject);
        else if (!(jsonObject instanceof Number))
            throw new SQLException( "value " +jsonObject+ " not a number");
        return null;
    }

    public void setBigDecimal(BigDecimal val) throws SQLException
    {
        if (val == null)
        {
            field = new Field(null,"null");
            isNull = true;
        }
        else
        {
            field = new Field(null, "number");
        }
        jsonObject = val;

    }

    @Override
    public void setFloat(float val) throws SQLException
    {
        field = new Field(null,"number" );
        jsonObject = val;

    }

    @Override
    public float getFloat() throws SQLException
    {
        if (jsonObject == null )
        {
            isNull = true;
            return 0;
        }
        else if (jsonObject instanceof Integer)

            return (float)((Integer)jsonObject);
        else if (jsonObject instanceof Long)
            return (float)((Long)jsonObject);
        else  if (jsonObject instanceof Short)
            return new Float((Short)jsonObject);
        else  if (jsonObject instanceof Byte)
            return (float)((Byte)jsonObject);
        else if (jsonObject instanceof Double)
            return ((Double) jsonObject).floatValue();
        else if (!(jsonObject instanceof Number))
            throw new SQLException( "value " +jsonObject+ " not a number");
        return 0;
    }

    @Override
    public void setDouble(double val) throws SQLException
    {
        field = new Field(null,"number" );
        jsonObject = val;

    }

    @Override
    public double getDouble() throws SQLException
    {
        if (jsonObject == null )
        {
            isNull = true;
            return 0;
        }
        else if (jsonObject instanceof Integer)
            return (double)((Integer)jsonObject);
        else if (jsonObject instanceof Long)
            return (double)((Long)jsonObject);
        else  if (jsonObject instanceof Short)
            return new Double((Short)jsonObject);
        else  if (jsonObject instanceof Byte)
            return (double)((Byte)jsonObject);
        else if (jsonObject instanceof Double)
            return (Double) jsonObject;
        else if (!(jsonObject instanceof Number))
            throw new SQLException( "value " +jsonObject+ " not a number");
        return 0;
    }

    @Override
    public void setBytes(byte[] val) throws SQLException
    {

    }

    @Override
    public byte[] getBytes() throws SQLException
    {
        return new byte[0];
    }

    @Override
    public void setDate(Date val, Calendar cal) throws SQLException
    {
        field = new Field(null,"string" );

        if (val == null)
        {
            jsonObject=null;
            isNull = true;
        }
        else
        {
            if (cal != null)
            {
                cal = (Calendar) cal.clone();
            }

            jsonObject = timestampUtils.toString(cal, val);

        }

    }

    @Override
    public Date getDate() throws SQLException
    {
        return null;
    }

    @Override
    public void setTime(Time val, Calendar cal) throws SQLException
    {
        field = new Field(null,"string" );

        if (val == null)
        {
            jsonObject=null;
            isNull=true;
        }
        else
        {
            if (cal != null)
            {
                cal = (Calendar) cal.clone();
            }

            jsonObject = timestampUtils.toString(cal, val);

        }
    }

    @Override
    public Time getTime() throws SQLException
    {
        return null;
    }

    @Override
    public void setTimestamp(Timestamp val, Calendar cal) throws SQLException
    {
        field = new Field(null,"string" );

        if (val == null)
        {
            jsonObject=null;
            isNull=true;
        }
        else
        {
            if (cal != null)
            {
                cal = (Calendar) cal.clone();
            }

            jsonObject = timestampUtils.toString(cal, val);

        }
    }

    @Override
    public Timestamp getTimestamp() throws SQLException
    {
        return null;
    }

    public Map getMap() throws SQLException
    {
        if (jsonObject == null )
        {
            isNull = true;
            return null;
        }
        if( jsonObject instanceof Map )
            return (Map)jsonObject;

        throw new SQLException("Value " +jsonObject + " is not a map" );
    }

    public void setMap(Map map) throws SQLException
    {
        if (map == null)
        {
            field = new Field(null,"null");
            isNull = true;
        }
        else
        {
            field = new Field(null, "map");
        }
        jsonObject = map;


    }

    @Override
    public List getArray() throws SQLException
    {
        if (jsonObject == null )
        {
            isNull = true;
            return null;
        }

        if ( jsonObject instanceof List )
            return (List) jsonObject;

        throw new SQLException("Value " +jsonObject + " is not a list" );

    }

    @Override
    public void setArray(List array) throws SQLException
    {
        if (array == null)
        {
            field = new Field(null,"null");
            isNull = true;
        }
        else
        {
            field = new Field(null, "array");
        }
        jsonObject = array;

    }

    public Object getObject() throws SQLException
    {
        switch (field.getSqlType())
        {
            case Types.NUMERIC:
                return new Double((String)jsonObject);
            case Types.BOOLEAN:
                return new Boolean((String)jsonObject);
            case Types.VARCHAR:
                Object object = jsonObject;
                if (object instanceof java.util.Date)
                {
                    return new java.sql.Date(((java.util.Date)object).getTime());
                }
                else
                {
                    return object;
                }
            case Types.ARRAY:
            case Types.JAVA_OBJECT:
                return jsonObject;

            case Types.NULL:
                return null;
        }
        return null;
    }

    public void setObject(Object x) throws SQLException
    {
        if (x == null)
        {
            field = new Field(null,"null");
            isNull = true;
        }
        else if (x instanceof String)
            setString((String)x);
        else if (x instanceof BigDecimal)
            setBigDecimal((BigDecimal)x);
        else if (x instanceof Short)
            setShort(((Short)x).shortValue());
        else if (x instanceof Integer)
            setInt(((Integer)x).intValue());
        else if (x instanceof Long)
            setLong(((Long)x).longValue());
        else if (x instanceof Float)
            setFloat(((Float)x).floatValue());
        else if (x instanceof Double)
            setDouble(((Double)x).doubleValue());
        else if (x instanceof byte[])
            setBytes((byte[])x);
        else if (x instanceof java.sql.Date)
            setDate((java.sql.Date)x, null);
        else if (x instanceof Time)
            setTime((Time)x, null);
        else if (x instanceof Timestamp)
            setTimestamp((Timestamp)x, null);
        else if (x instanceof Boolean)
            setBoolean(((Boolean)x).booleanValue());
        else if (x instanceof Byte)
            setByte(((Byte)x).byteValue());
        else if (x instanceof Character)
            setString(((Character) x).toString());
        else if (x instanceof Map)
            setMap((Map) x);
        else
        {
            // Can't infer a type.
            throw new SQLException("Can''t infer the SQL type to use for an instance of " + x +". Use setObject() with an explicit Types value to specify the type to use.");
        }
    }
    public boolean isNull() throws SQLException
    {
        return isNull;
    }

    public int getJDBCType()
    {
        return jdbcTypes.get(field.getType());
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

    static final String QUOTE="\"";

    public String parameterValue()
    {
        if (jsonObject == null )
            return "null";
        else if (jsonObject instanceof String)
            return QUOTE + jsonObject + QUOTE;
        else if (jsonObject instanceof Map)
            return  JsonFactory.toJson(jsonObject);
        else if (jsonObject instanceof List)
            return  JsonFactory.toJson(jsonObject);
        else
            return jsonObject.toString();
    }
}
