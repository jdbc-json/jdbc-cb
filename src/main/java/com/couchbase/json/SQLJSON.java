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

package com.couchbase.json;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * Created by davec on 2015-06-26.
 */
public interface SQLJSON
{
    void free();

    InputStream getBinaryStream() throws SQLException;
    void setBinaryStream() throws SQLException;

    Reader getCharacterStream() throws SQLException;
    void setCharacterStream(Reader stream) throws SQLException;

    String getString() throws SQLException;
    void setString(String str) throws SQLException;

    boolean getBoolean() throws SQLException;
    void setBoolean(boolean val) throws SQLException;

    byte getByte() throws  SQLException;
    void setByte(byte val) throws SQLException;

    short getShort() throws SQLException;
    void setShort(short val) throws SQLException;

    int getInt() throws SQLException;
    void setInt(int val) throws SQLException;

    long getLong() throws SQLException;
    void setLong(long val) throws SQLException;

    void setFloat(float val)throws SQLException;
    float getFloat()throws SQLException;

    void setDouble(double val)throws SQLException;
    double getDouble()throws SQLException;

    void setBytes(byte[] val)throws SQLException;
    byte[] getBytes()throws SQLException;

    void setDate(Date val, Calendar cal)throws SQLException;
    Date getDate(Calendar cal)throws SQLException;

    void setTime(Time val, Calendar cal)throws SQLException;
    Time getTime(Calendar cal)throws SQLException;

    void setTimestamp(Timestamp val, Calendar cal)throws SQLException;
    Timestamp getTimestamp(Calendar cal)throws SQLException;

    BigDecimal getBigDecimal() throws SQLException;
    void setBigDecimal(BigDecimal val) throws SQLException;

    Map getMap() throws SQLException;
    void setMap(Map map) throws SQLException;

    List getArray() throws SQLException;
    void setArray(Object[] array) throws SQLException;
    void setArray(List array) throws SQLException;

    Object getObject() throws SQLException;
    void setObject(Object val) throws SQLException;

    /*
     *
     * @param columnName
     * @return  if the SQLJSON object is a JSON object.
             Get the given object with the given fieldName,
     * @throws SQLException   Throw not valid exception if the SQLJSON object is not a JSON object.
     */
    Object getObject(String columnName) throws SQLException;


    /*
     *
     * Set the given object with the given fieldName, if the SQLJSON object is a JSON object.
     * @param columnName
     * @param val
     * @throws SQLException  Throw not valid exception if the SQLJSON object is not a JSON object.
     */
    void setObject(String columnName, Object val) throws SQLException;


    /*
     * Return the object at the given index, if the SQLJSON object is a JSON array.
     * Return NULL if the SQLJSON object is not a JSON array
     * or if the SQLJSON object is a JSON array and does not have an element at the given index.
     *
     * @param index
     * @return
     */
    Object get(int index) ;

    /*
     * Set the given index with the given element, if the SQLJSON object is a JSON array.
     *
     * @param index
     */
    void set(int index, Object object) throws SQLException;
    boolean isNull() throws SQLException;

    int getJDBCType();
    Object parameterValue();


    Object parse(Class clazz) throws SQLException;
    Map parse() throws SQLException;

    int compareTo(SQLJSON sqljson);
}
