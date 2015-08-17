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
    public void free();

    public InputStream getBinaryStream() throws SQLException;
    public void setBinaryStream() throws SQLException;

    public Reader getCharacterStream() throws SQLException;
    public void setCharacterStream(Reader stream) throws SQLException;

    public String getString() throws SQLException;
    public void setString(String str) throws SQLException;

    public boolean getBoolean() throws SQLException;
    public void setBoolean(boolean val) throws SQLException;

    public byte getByte() throws  SQLException;
    public void setByte(byte val ) throws SQLException;

    public short getShort() throws SQLException;
    public void setShort(short val) throws SQLException;

    public int getInt() throws SQLException;
    public void setInt(int val) throws SQLException;

    public long getLong() throws SQLException;
    public void setLong(long val) throws SQLException;

    public void setFloat(float val)throws SQLException;
    public float getFloat()throws SQLException;

    public void setDouble( double val )throws SQLException;
    public double getDouble()throws SQLException;

    public void setBytes( byte[] val)throws SQLException;
    public byte[] getBytes()throws SQLException;

    public void setDate(Date val, Calendar cal)throws SQLException;
    public Date getDate(Calendar cal)throws SQLException;

    public void setTime( Time val, Calendar cal )throws SQLException;
    public Time getTime(Calendar cal)throws SQLException;

    public void setTimestamp( Timestamp val, Calendar cal)throws SQLException;
    public Timestamp getTimestamp(Calendar cal)throws SQLException;

    public BigDecimal getBigDecimal() throws SQLException;
    public void setBigDecimal(BigDecimal val) throws SQLException;

    public Map getMap() throws SQLException;
    public void setMap(Map map) throws SQLException;

    public List getArray() throws SQLException;
    public void setArray(Object []array) throws SQLException;
    public void setArray(List array) throws SQLException;

    public Object getObject() throws SQLException;
    public void setObject(Object val) throws SQLException;

    public boolean isNull() throws SQLException;

    public int getJDBCType();
    public String parameterValue();


    public Object parse(Class clazz) throws SQLException;
    public Map parse() throws SQLException;

    public int compareTo(SQLJSON sqljson);
}
