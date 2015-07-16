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

package com.couchbase;

import com.couchbase.jdbc.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.InputStream;
import java.io.Reader;
import java.sql.*;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by davec on 2015-07-15.
 */
public class ResultSetTest
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    Connection con;

    @Before
    public void openConnection() throws Exception
    {
        con = DriverManager.getConnection(TestUtil.getURL(), TestUtil.getUser(), TestUtil.getPassword());
        assertNotNull(con);
        con.createStatement().executeUpdate("delete from test1");
        System.out.print("connection opened");
    }

    @After
    public void closeConnection() throws Exception
    {
        assertNotNull(con);

        try(Statement statement = con.createStatement())
        {
            statement.executeUpdate("delete from test1");
        }

        con.close();
    }

    @Test
    public void testNotImplemented() throws Exception
    {
        try(Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);

                rs.absolute(0);
                rs.first();
                rs.last();

                rs.afterLast();
                rs.beforeFirst();
                rs.isAfterLast();
                rs.isBeforeFirst();

                rs.refreshRow();
                rs.relative(1);

                rs.updateArray(1,null);
                rs.updateArray("foo",null);

                rs.updateAsciiStream(1,null);
                rs.updateAsciiStream(1,null,(int)0);
                rs.updateAsciiStream(1, null, (long)0);

                rs.updateAsciiStream("foo", null);
                rs.updateAsciiStream("foo", null, 0);
                rs.updateAsciiStream("foo", null, (long)0);

                rs.updateBigDecimal(1, null);
                rs.updateBigDecimal("foo", null);

                rs.updateBinaryStream(1, null);
                rs.updateBinaryStream(1, null, 0);
                rs.updateBinaryStream(1, null, (long)0);

                rs.updateBinaryStream("foo",null);
                rs.updateBinaryStream("foo",null, 0);
                rs.updateBinaryStream("foo",null, 0);

                rs.updateBlob(1, (Blob)null);
                rs.updateBlob(1, (InputStream)null);
                rs.updateBlob(1, null, 0);

                rs.updateBlob("foo", (Blob)null);
                rs.updateBlob("foo", (InputStream)null);
                rs.updateBlob("foo", null, 0);

                rs.updateBoolean(1,true);
                rs.updateBoolean("foo",true);

                rs.updateByte(1,(byte)0);
                rs.updateByte("foo",(byte)0);

                rs.updateBytes(1, null);
                rs.updateBytes("foo", null);

                rs.updateCharacterStream(1, null);
                rs.updateCharacterStream(1, null, 0);
                rs.updateCharacterStream(1, null, (long)0);

                rs.updateCharacterStream("foo", null);
                rs.updateCharacterStream("foo", null, 0);
                rs.updateCharacterStream("foo", null, (long)0);

                rs.updateClob(1, (Clob)null);
                rs.updateClob(1, null, 0);

                rs.updateClob("foo", (Clob)null);
                rs.updateClob("foo", null, 0);

                rs.updateNCharacterStream(1, null);
                rs.updateNCharacterStream(1, null, 0);

                rs.updateNCharacterStream("foo", null);
                rs.updateNCharacterStream("foo", null, 0);

                rs.updateNClob(1, (NClob)null);
                rs.updateNClob(1, (Reader)null);
                rs.updateNClob(1, null, 0);

                rs.updateNClob("foo", (NClob)null);
                rs.updateNClob("foo", (Reader)null);
                rs.updateNClob("foo", null, 0);

                rs.updateNString(1, null);
                rs.updateNString("foo", null);

                rs.updateNull(1);
                rs.updateNull("foo");

                rs.updateDate(1, null);
                rs.updateDate("foo", null);

                rs.updateDouble(1, 0);
                rs.updateDouble("foo", 0);

                rs.updateFloat(1,0);
                rs.updateFloat("foo",0);

                rs.updateInt(1, 0);
                rs.updateInt("foo",0);

                rs.updateLong(1, 0);
                rs.updateLong("foo", 0);

                rs.updateObject(1, null);
                rs.updateObject(1, null, 0);

                rs.updateObject("foo", null);
                rs.updateObject("foo", null, 0);

                rs.updateRef(1, null);
                rs.updateRef("foo", null);

                rs.updateRow();

                rs.updateRowId(1, null);
                rs.updateRowId("foo", null);

                rs.updateTime(1, null);
                rs.updateTime("foo", null);

                rs.updateTimestamp(1, null);
                rs.updateTimestamp("foo", null);


            }
        }
    }
    @Test
    public void testGetDate() throws Exception
    {
        try (Statement stmt = con.createStatement())
        {
            try (ResultSet rs = stmt.executeQuery("select now_str() as cur_time"))
            {
                assertTrue(rs.next());
                Date date = rs.getDate("cur_time");
                assertNotNull(date);

                date = rs.getDate(1);
                assertNotNull(date);

                date = (Date)rs.getObject("cur_time");
                assertNotNull(date);

                date = (Date)rs.getObject(1);
                assertNotNull(date);



            }
        }
    }

    @Test
    public void testGetTimestamp() throws Exception
    {
        try (Statement stmt = con.createStatement())
        {
            try (ResultSet rs = stmt.executeQuery("select now_str() as cur_time"))
            {
                assertTrue(rs.next());
                Timestamp ts = rs.getTimestamp("cur_time");
                assertNotNull(ts);
                ts=rs.getTimestamp(1);
                assertNotNull(ts);

                ts=rs.getObject("cur_time", Timestamp.class);
                assertNotNull(ts);
                ts=rs.getObject(1, Timestamp.class);
                assertNotNull(ts);
            }
        }
    }
}
