package com.couchbase.jdbc.core;

import com.couchbase.jdbc.util.JSONTypes;
import com.couchbase.jdbc.util.TimestampUtils;
import com.couchbase.json.SQLJSON;
import org.boon.core.reflection.Mapper;
import org.boon.core.reflection.MapperSimple;
import org.boon.core.value.ValueList;
import org.boon.json.JsonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.*;
import java.util.*;

/**
 * Created by davec on 15-07-02.
 */
public class SqlJsonImplementation implements SQLJSON
{
    private static final Logger logger = LoggerFactory.getLogger(SqlJsonImplementation.class);
    Field field;
    Object jsonObject;
    String sqlJson;
    boolean isNull = false;
    TimestampUtils timestampUtils = new TimestampUtils();



    public SqlJsonImplementation()
    {

    }
    public SqlJsonImplementation(Object jsonObject, Field field )
    {
        this.jsonObject = jsonObject;
        if (jsonObject instanceof String)
            field.setType("string");
        else if (jsonObject instanceof Number)
            field.setType("number");
        else if (jsonObject instanceof Boolean)
            field.setType("boolean");
        else if (jsonObject instanceof List)
            field.setType("array");
        // map should be json as well
        else
            field.setType("json");

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
            logger.error("Error Encoding", ex );
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
        int type = JSONTypes.jsonTypes.get(field.getType());
        switch (type)
        {
            case JSONTypes.JSON_ARRAY:
            case JSONTypes.JSON_OBJECT:
            case JSONTypes.JSON_MAP:
                toJson();
                return sqlJson;
            case JSONTypes.JSON_NULL:
                return null;
            case JSONTypes.JSON_STRING:
                if ( sqlJson != null ) return sqlJson;
                if ( jsonObject != null ) return (String) jsonObject;
                isNull = true;
                return null;

            // let the default implementation of toString figure it out
            case JSONTypes.JSON_BOOLEAN:
            case JSONTypes.JSON_NUMBER:
                return jsonObject.toString();
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
        int type = JSONTypes.jsonTypes.get(field.getType());

        switch (type)
        {
            case JSONTypes.JSON_BOOLEAN:
                return (boolean) jsonObject;
            case JSONTypes.JSON_NUMBER:
                Number number = (Number)jsonObject;
                return !number.equals((Number)0);
            case JSONTypes.JSON_STRING:
                String string = (String) jsonObject;
                return !string.isEmpty();
            case JSONTypes.JSON_MAP:
            case JSONTypes.JSON_OBJECT:
                Map map = (Map) jsonObject;
                return !map.isEmpty();
            case JSONTypes.JSON_ARRAY:
                List list = (List)jsonObject;
                return !list.isEmpty();

            default:
                return false;
        }

    }

    public void setBoolean(boolean val) throws SQLException
    {
        field = new Field(null,"boolean" );
        jsonObject = val;
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
    public Date getDate(Calendar cal) throws SQLException
    {
        Date date = null;

        if (jsonObject == null )
        {
            isNull = true;
            return null;
        }

        try
        {
            if ( jsonObject instanceof String)
            {
                date = timestampUtils.parse((String)jsonObject);
            }
            if (jsonObject instanceof Date)
            {
                date = (Date)jsonObject;
            }
            if (jsonObject instanceof java.util.Date)
            {
                date = new Date(((java.util.Date)jsonObject).getTime());
            }
        }
        catch (Exception ex)
        {
            throw new SQLException("value " + jsonObject + " is not a date");
        }

        if ( cal!= null )
        {
            date = timestampUtils.applyCalendar(cal, date);
        }

        return date;
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
    public Time getTime(Calendar cal) throws SQLException
    {
        Time time;

        if ( jsonObject == null ) return null;

        try
        {
            time = timestampUtils.parseTime((String)jsonObject);
        }
        catch( Exception ex)
        {
            throw new SQLException("value " + jsonObject +" is not a Time", ex);
        }

        if ( cal != null )
        {
            time = timestampUtils.applyCalendar(cal, time);
        }
        return time;
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
    public Timestamp getTimestamp( Calendar cal ) throws SQLException
    {
        Timestamp ts;

        if (jsonObject == null )
        {
            isNull=true;
            return null;
        }

        try
        {
            if (jsonObject instanceof java.util.Date || jsonObject instanceof java.sql.Date)
            {
                ts = new Timestamp( ((java.util.Date)jsonObject).getTime());
            }
            else if (jsonObject instanceof String)
            {
                ts = timestampUtils.parseTimestamp((String)jsonObject);
            }
            else
            {
                throw new SQLException("value " + jsonObject + " is not a Timestamp");
            }
        }
        catch( Exception ex)
        {
            throw new SQLException("value " + jsonObject+ "is not a Timestamp", ex);
        }

        if ( cal != null)
        {
            ts = timestampUtils.applyCalendar(cal,ts);
        }
        /*
        // check to see if there is a calendar and that it is different than the one used to parse
        */
        return ts;
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
        else
            return null;


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

    @Override
    public void setArray(Object []array) throws SQLException
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
            case Types.BOOLEAN:
                    return jsonObject;
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
            setString(x.toString());
        else if ( x instanceof List)
            setArray((List)x);
        else if ( x instanceof Object [] )
            setArray((Object [])x);
        else if (x.getClass().isArray())
        {
            List list = asList(x);
            setArray(list);

        }
        else if (x instanceof Map)
            setMap((Map) x);
        else
        {
            // Can't infer a type.
            throw new SQLException("Can''t infer the SQL type to use for an instance of " + x +". Use setObject() with an explicit Types value to specify the type to use.");
        }
    }

    /*
     * @param columnName
     * @return if the SQLJSON object is a JSON object.
     * Get the given object with the given fieldName,
     * @throws java.sql.SQLException Throw not valid exception if the SQLJSON object is not a JSON object.
     */
    @Override
    public Object getObject(String columnName) throws SQLException
    {
        if (!(jsonObject instanceof Map))
        {
            throw new SQLException("Object is not a json object");
        }
        else
        {
            return ((Map)jsonObject).get(columnName);
        }

    }


    /*
     * Set the given object with the given fieldName, if the SQLJSON object is a JSON object.
     *
     * @param columnName
     * @param val
     * @throws java.sql.SQLException Throw not valid exception if the SQLJSON object is not a JSON object.
     */
    @Override
    public void setObject(String columnName, Object val) throws SQLException
    {
        if (!(jsonObject instanceof Map))
        {
            throw new SQLException("Object is not a json object");
        }
        else
        {
            //noinspection unchecked
            ((Map)jsonObject).put(columnName, val);
        }

    }


    /*
     * Return the object at the given index, if the SQLJSON object is a JSON array.
     * Return NULL if the SQLJSON object is not a JSON array
     * or if the SQLJSON object is a JSON array and does not have an element at the given index.
     *
     * @param index
     * @return
     */
    @Override
    public Object get(int index)
    {
        if (!(jsonObject instanceof List))
        {
            return null;
        }
        else
        {
            return ((List)jsonObject).get(index);
        }
    }

    /*
     * Set the given index with the given element, if the SQLJSON object is a JSON array.
     *
     * @param index
     * @param object
     */
    @Override
    public void set(int index, Object object) throws SQLException
    {
        if (!(jsonObject instanceof List))
        {
            throw new SQLException("SQLJSON object is not a list");
        }
        else
        {
            // this is a hack
            List backingList = ((ValueList) jsonObject).list();
            //noinspection unchecked
            backingList.set(index,object);
        }
    }

    public boolean isNull() throws SQLException
    {
        return isNull;
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> asList(final Object array) {
        if (!array.getClass().isArray())
            throw new IllegalArgumentException("Not an array");
        return new AbstractList<T>() {
            @Override
            public T get(int index) {
                return (T) java.lang.reflect.Array.get(array, index);
            }

            @Override
            public int size() {
                return java.lang.reflect.Array.getLength(array);
            }
        };
    }
    public int getJDBCType()
    {
        return JSONTypes.jdbcTypes.get(field.getType());
    }

    @Override
    public Object parse(Class clazz)
    {
        Mapper mapper = new MapperSimple();
        //noinspection unchecked
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
            if (jsonObject instanceof String)
            {
                logger.debug("json object is string " + jsonObject);
                return null;
            }
            else
                return (Map)jsonObject;
        }
        else return null;
    }

    public Object parameterValue()
    {
        return jsonObject;
    }
    public int getLength()
    {
        return sqlJson.length();
    }
    public int compareTo(SQLJSON obj)
    {

        SqlJsonImplementation sqljson = (SqlJsonImplementation)obj;

        int deltaLength = this.getLength() - sqljson.getLength();
        if ( deltaLength != 0 ) return deltaLength;

        // if not do name by name comparison
        if (jsonObject instanceof Map && sqljson.jsonObject instanceof Map)
        {
            @SuppressWarnings("unchecked") Set<String> combinedKeys = ((Map)jsonObject).keySet();
            //noinspection unchecked
            combinedKeys.addAll(((Map) sqljson.jsonObject).keySet());
            List <String> sorted = new ArrayList<String>(combinedKeys);
            Collections.sort(sorted);

            for(String name:sorted)
            {
                // this did not have the value so it is larger
                if ( !((Map)jsonObject).containsKey(name) ) return 1;

                // this did not have the value so it is larger
                if ( !((Map)sqljson.jsonObject).containsKey(name) ) return -1;

                Object obj1 = ((Map)jsonObject).get(name);
                Object obj2 = ((Map)sqljson.jsonObject).get(name);

                return compare(obj1, obj2);

            }
            return 0;
        }

        if (jsonObject instanceof List && sqljson.jsonObject instanceof List)
        {
            //assume they are the same length from the test above
            for (int i =0; i< ((List) jsonObject).size();i++)
            {
                Object obj1 = ((List)jsonObject).get(i);
                Object obj2 = ((List)sqljson.jsonObject).get(i);
                return compare(obj1, obj2);
            }


        }
        return 0;
    }
    private int compare(Object obj1, Object obj2)
    {
        if (obj1 == null && obj2 == null ) return 0;

        if ( obj1.getClass().isInstance(obj2))
        {
            if (obj1 instanceof String) return ((String) obj1).compareTo((String)obj2);
            if (obj1 instanceof Boolean)
            {
                if (obj1 == obj2) return 0;
                if (obj1 == Boolean.TRUE) return 1;
                return -1;
            }
            if (obj1 instanceof java.util.Date ) return ((java.util.Date) obj1).compareTo((Date)obj2);
            if (obj1 instanceof Number)
            {
                double number1 = ((Number)obj1).doubleValue();
                double number2 = ((Number)obj2).doubleValue();

                if (number1 == number2) return 0;
                return (number1>number2?1:-1);
            }
        }
        logger.debug("should not get here");
        return 0;
    }
}
