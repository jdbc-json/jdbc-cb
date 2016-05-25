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

package com.couchbase.jdbc;

import com.couchbase.json.SQLJSON;
import org.boon.json.JsonFactory;
import org.boon.json.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.After;
import org.junit.Ignore;

import java.math.BigDecimal;

import com.couchbase.jdbc.CBConnection;
import com.couchbase.jdbc.CBPreparedStatement;
import com.couchbase.jdbc.CBResultSet;

import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * Created by davec on 2015-07-10.
 */
@RunWith(JUnit4.class)
public class TestSQLJson extends CouchBaseTestCase
{

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @After
	public void cleanupBucket() throws Exception
	{
		JDBCTestUtils.deleteDataFromBucket("default");
	}
    
    @Test
    public void getSqlJson() throws Exception
    {
        String query = "SELECT * FROM default limit 10";

        try (Statement stmt= con.createStatement())
        {
            CBResultSet rs = (CBResultSet)stmt.executeQuery(query);
            assertNotNull(rs);

            while (rs.next())
            {
                SQLJSON sqljson = rs.getSQLJSON(1);
                Object obj = sqljson.getObject();
                assertNotNull(obj);
            }
        }
    }

    @Test
    public void setSqlJson() throws Exception
    {
        String json = "{\n" +
                "  \"emailAddress\": \"jakayla@crooks.info\",\n" +
                "  \"type\": \"customer\",\n" +
                "  \"dateLastActive\": \"2014-05-06T15:52:14Z\",\n" +
                "  \"firstName\": \"Darrin\",\n" +
                "  \"phoneNumber\": \"497-854-2229 x000\",\n" +
                "  \"postalCode\": \"45603-9112\",\n" +
                "  \"lastName\": \"Ortiz\",\n" +
                "  \"ccInfo\": {\n" +
                "    \"cardNumber\": \"1234-2121-1221-1211\",\n" +
                "    \"cardType\": \"discover\",\n" +
                "    \"cardExpiry\": \"2012-11-12\"\n" +
                "  },\n" +
                "  \"dateAdded\": \"2013-06-10T15:52:14Z\",\n" +
                "  \"state\": \"IN\",\n" +
                "  \"customerId\": \"customer10\"\n" +
                "}";

        ObjectMapper mapper = JsonFactory.create();
        Map <String,Object> jsonObject = mapper.readValue(json, Map.class);

        String query = "insert into default (key,value) values (?,?)";

        SQLJSON sqljson = ((CBConnection)con).createSQLJSON();
        sqljson.setMap( jsonObject );
        try (PreparedStatement pstmt = con.prepareStatement(query))
        {
            pstmt.setString(1,"customer1");
            ((CBPreparedStatement)pstmt).setSQLJSON(2,sqljson);
            assertEquals(1,pstmt.executeUpdate());
        }
    }

    @Test
    public void testGetString() throws Exception
    {
        String query = "SELECT true as c1, false as c2, 0 as c3, 1 as c4, '' as c5, 'some' as c6, [1,2,3,5,8] as c7, [] as c8, { 'a1': 'Object' } as c9, {} as c10";

        try (Statement stmt = con.createStatement())
        {
            CBResultSet rs = (CBResultSet) stmt.executeQuery(query);
            assertNotNull(rs);

            assertTrue(rs.next());

            SQLJSON sqljson = rs.getSQLJSON("c1");
            assertEquals("true", sqljson.getString());

            sqljson = rs.getSQLJSON("c2");
            assertEquals("false", sqljson.getString());

            sqljson = rs.getSQLJSON("c7");
            List list = sqljson.getArray();

            assertNotNull(list);


        }
    }
    @Test
    public void testSetString() throws Exception
    {
        SQLJSON sqljson = ((CBConnection)con).createSQLJSON();
        sqljson.setString("a string");

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1, "val1");
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1, "val2");
            sqljson.setString(null);
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());


            try (Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select default from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals("a string", sqljson1.getString());
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val2'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertNull(sqljson1);
                    assertTrue(rs.wasNull());
                }
            }
        }
    }

    @Test
    public void testSetArray()  throws Exception
    {
        SQLJSON sqljson = ((CBConnection)con).createSQLJSON();
        List list = new ArrayList();

        for (int i=0; i<6;i++)
            list.add(i,i+1);

        sqljson.setArray(list);

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1, "val1");
            ((CBPreparedStatement) preparedStatement).setSQLJSON(2, sqljson);
            preparedStatement.execute();

        }

        try (Statement statement = con.createStatement())
        {
            try (ResultSet rs = statement.executeQuery("select default from default where meta(default).id='val1'"))
            {
                assertTrue(rs.next());

                SQLJSON sqljson1 = ((CBResultSet) rs).getSQLJSON("default");
                List returned = sqljson1.getArray();
                for (int i=0; i<6;i++)
                    assertEquals(i+1,returned.get(i));
            }
        }
    }

    @Test
    public void testGetBoolean() throws Exception
    {
        String query = "SELECT true as c1, false as c2, 0 as c3, 1 as c4, '' as c5, 'some' as c6, [1,2,3,5,8] as c7, [] as c8, { 'a1': 'Object' } as c9, {} as c10";

        try (Statement stmt= con.createStatement())
        {
            CBResultSet rs = (CBResultSet)stmt.executeQuery(query);
            assertNotNull(rs);

            assertTrue(rs.next());


            SQLJSON sqljson = rs.getSQLJSON("c1");
            assertTrue(sqljson.getBoolean());

            sqljson = rs.getSQLJSON("c2");
            assertFalse(sqljson.getBoolean());

             sqljson = rs.getSQLJSON("c3");
            assertFalse(sqljson.getBoolean());

            sqljson = rs.getSQLJSON("c4");
            assertTrue(sqljson.getBoolean());

            sqljson = rs.getSQLJSON("c5");
            assertFalse(sqljson.getBoolean());

            sqljson = rs.getSQLJSON("c6");
            assertTrue(sqljson.getBoolean());

            sqljson = rs.getSQLJSON("c8");
            assertFalse(sqljson.getBoolean());

            sqljson = rs.getSQLJSON("c7");
            assertTrue(sqljson.getBoolean());

            sqljson = rs.getSQLJSON("c10");
            assertFalse(sqljson.getBoolean());

            sqljson = rs.getSQLJSON("c9");
            assertTrue(sqljson.getBoolean());



        }
    }

    @Test
    public void testSetBoolean() throws Exception
    {

        SQLJSON sqljson = ((CBConnection)con).createSQLJSON();
        sqljson.setBoolean(true);

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1, "val1");
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1, "val2");
            sqljson.setBoolean(false);
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());


            try (Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select default from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertTrue(sqljson1.getBoolean());
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val2'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertFalse(sqljson1.getBoolean());
                }
            }
        }
    }

    @Test
    public void testGetByte() throws Exception
    {
        String query = "SELECT 0 as c1, 1 as c2, 65535 as c3, '1.0' as c4, null as c5";

        try (Statement stmt= con.createStatement())
        {
            CBResultSet rs = (CBResultSet) stmt.executeQuery(query);
            assertNotNull(rs);

            assertTrue(rs.next());


            SQLJSON sqljson = rs.getSQLJSON("c1");
            assertEquals((byte)0,sqljson.getByte());

            sqljson = rs.getSQLJSON("c2");
            assertEquals((byte) 1, sqljson.getByte());

            sqljson = rs.getSQLJSON("c3");
            assertEquals((byte) -1, sqljson.getByte());

            sqljson = rs.getSQLJSON("c4");
            expectedException.expect(SQLException.class);
            assertEquals((byte) 1, sqljson.getByte());

            sqljson = rs.getSQLJSON("c5");
            assertNull(sqljson);
            assertTrue(rs.wasNull());



        }
    }

    @Test
    public void testSetByte() throws Exception
    {
        SQLJSON sqljson = ((CBConnection)con).createSQLJSON();
        sqljson.setByte((byte)1);

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1, "val1");
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1, "val2");
            sqljson.setByte((byte)0);
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            try (Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select default from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals(1,sqljson1.getByte());
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val2'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals(0,sqljson1.getByte());
                }

            }
        }

    }

    @Test
    public void testGetShort() throws Exception
    {
        String query = "SELECT 0 as c1, 1 as c2, 65535 as c3, '1.0' as c4, null as c5";

        try (Statement stmt= con.createStatement())
        {
            CBResultSet rs = (CBResultSet) stmt.executeQuery(query);
            assertNotNull(rs);

            assertTrue(rs.next());


            SQLJSON sqljson = rs.getSQLJSON("c1");
            assertEquals(0,sqljson.getShort());

            sqljson = rs.getSQLJSON("c2");
            assertEquals( 1, sqljson.getShort());

            sqljson = rs.getSQLJSON("c3");
            assertEquals( -1, sqljson.getShort());

            sqljson = rs.getSQLJSON("c4");
            expectedException.expect(SQLException.class);
            assertEquals( 1, sqljson.getShort());

            sqljson = rs.getSQLJSON("c5");
            assertNull(sqljson);
            assertTrue(rs.wasNull());

        }
    }

    @Test
    public void testSetShort() throws Exception
    {
        SQLJSON sqljson = ((CBConnection)con).createSQLJSON();
        sqljson.setShort(Short.MAX_VALUE);

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1, "val1");
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1, "val2");
            sqljson.setShort((short) 0);
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            try (Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select default from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals( Short.MAX_VALUE, sqljson1.getShort());
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val2'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals(0, sqljson1.getShort());
                }

            }
        }
    }

    @Test
    public void testGetInt() throws Exception
    {
        String query = "SELECT 0 as c1, 1 as c2," + Integer.MAX_VALUE + " as c3, '1.0' as c4, null as c5";

        try (Statement stmt= con.createStatement())
        {
            CBResultSet rs = (CBResultSet) stmt.executeQuery(query);
            assertNotNull(rs);

            assertTrue(rs.next());


            SQLJSON sqljson = rs.getSQLJSON("c1");
            assertEquals(0,sqljson.getInt());

            sqljson = rs.getSQLJSON("c2");
            assertEquals( 1, sqljson.getInt());

            sqljson = rs.getSQLJSON("c3");
            assertEquals( Integer.MAX_VALUE, sqljson.getInt());

            sqljson = rs.getSQLJSON("c4");
            expectedException.expect(SQLException.class);
            assertEquals( 1, sqljson.getInt());

            sqljson = rs.getSQLJSON("c5");
            assertNull(sqljson);
            assertTrue(rs.wasNull());

        }

    }

    @Test
    public void testSetInt() throws Exception
    {
        SQLJSON sqljson = ((CBConnection)con).createSQLJSON();
        sqljson.setInt(Integer.MAX_VALUE);

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1, "val1");
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1, "val2");
            sqljson.setInt(0);
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1, "val3");
            sqljson.setInt(Integer.MIN_VALUE);
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());


            try (Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select default from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals(Integer.MAX_VALUE,sqljson1.getInt());
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val2'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals(0,sqljson1.getInt());
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val3'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals(Integer.MIN_VALUE,sqljson1.getInt());
                }

            }
        }
    }

    @Ignore("Known problem with maximal values.")
    @Test
    public void testGetLong() throws Exception
    {

        String query = "SELECT 0 as c1, 1 as c2, " + Long.MAX_VALUE + " as c3, '1.0' as c4, null as c5";

        try (Statement stmt= con.createStatement())
        {
            CBResultSet rs = (CBResultSet) stmt.executeQuery(query);
            assertNotNull(rs);

            assertTrue(rs.next());

            SQLJSON sqljson = rs.getSQLJSON("c1");
            assertEquals(0,sqljson.getLong());

            sqljson = rs.getSQLJSON("c2");
            assertEquals(1, sqljson.getLong());

            sqljson = rs.getSQLJSON("c3");
            assertEquals(Long.MAX_VALUE, sqljson.getLong());

            sqljson = rs.getSQLJSON("c4");
            expectedException.expect(SQLException.class);
            assertEquals( 1, sqljson.getLong());

            sqljson = rs.getSQLJSON("c5");
            assertNull(sqljson);
            assertTrue(rs.wasNull());

        }
    }

    @Ignore("Known problem with maximal values.")
    @Test
    public void testSetLong() throws Exception
    {
        SQLJSON sqljson = ((CBConnection)con).createSQLJSON();
        sqljson.setLong(Long.MAX_VALUE);

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1, "val1");
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1, "val2");
            sqljson.setLong((long) 0);
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1, "val3");
            sqljson.setLong(Long.MIN_VALUE);
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            try (Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select default from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals(Long.MAX_VALUE,sqljson1.getLong());
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val2'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals(0,sqljson1.getLong());
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val3'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals(Long.MIN_VALUE,sqljson1.getLong());
                }

            }
        }
    }

    @Ignore("Known problem with maximal values.")
    @Test
    public void testGetBigDecimal() throws Exception
    {
        String query = "SELECT 0 as c1, 1 as c2, " + BigDecimal.valueOf(Double.MAX_VALUE) + " as c3, " + BigDecimal.valueOf(Double.MIN_VALUE) + " as c4, null as c5";

        try (Statement stmt= con.createStatement())
        {
            CBResultSet rs = (CBResultSet) stmt.executeQuery(query);
            assertNotNull(rs);

            assertTrue(rs.next());


            SQLJSON sqljson = rs.getSQLJSON("c1");
            assertEquals(BigDecimal.ZERO,sqljson.getBigDecimal());

            sqljson = rs.getSQLJSON("c2");
            assertEquals(BigDecimal.ONE, sqljson.getBigDecimal());

            sqljson = rs.getSQLJSON("c3");
            assertEquals( BigDecimal.valueOf(Double.MAX_VALUE), sqljson.getBigDecimal());

            sqljson = rs.getSQLJSON("c4");
            assertEquals( BigDecimal.valueOf(Double.MIN_VALUE), sqljson.getBigDecimal());

            sqljson = rs.getSQLJSON("c5");
            assertNull(sqljson);
            assertTrue(rs.wasNull());

        }
    }

    @Ignore("Known problem with maximal values.")
    @Test
    public void testSetBigDecimal() throws Exception
    {
        SQLJSON sqljson = ((CBConnection)con).createSQLJSON();
        sqljson.setBigDecimal(BigDecimal.valueOf(Double.MAX_VALUE));

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1, "val1");
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1, "val2");
            sqljson.setBigDecimal( BigDecimal.ZERO );
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1, "val3");
            sqljson.setBigDecimal(BigDecimal.valueOf(Double.MIN_VALUE));
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            try (Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select default from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals(BigDecimal.valueOf(Double.MAX_VALUE),sqljson1.getBigDecimal());
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val2'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals(BigDecimal.ZERO,sqljson1.getBigDecimal());
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val3'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals(BigDecimal.valueOf(Double.MIN_VALUE),sqljson1.getBigDecimal());
                }

            }
        }
    }
    
    @Ignore("Known problem with maximal values.")
    @Test
    public void testSetFloat() throws Exception
    {
        SQLJSON sqljson = ((CBConnection)con).createSQLJSON();
        sqljson.setFloat(Float.MAX_VALUE);

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1, "val1");
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1, "val2");
            sqljson.setFloat(0);
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1, "val3");
            sqljson.setFloat(Float.MIN_VALUE);
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            try (Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select default from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals(Float.MAX_VALUE,sqljson1.getFloat());
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val2'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals((float)0.0,sqljson1.getFloat());
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val3'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals( Float.MIN_VALUE,sqljson1.getFloat());
                }

            }
        }
    }

    @Ignore("Known problem with maximal values.")
    @Test
    public void testGetFloat() throws Exception
    {
        String query = "SELECT 0 as c1, 1 as c2, " + Float.valueOf(Float.MAX_VALUE) + " as c3, " + Float.valueOf(Float.MIN_VALUE) + " as c4, null as c5";

        try (Statement stmt= con.createStatement())
        {
            CBResultSet rs = (CBResultSet) stmt.executeQuery(query);
            assertNotNull(rs);

            assertTrue(rs.next());


            SQLJSON sqljson = rs.getSQLJSON("c1");
            assertEquals((float)0.0,sqljson.getFloat());

            sqljson = rs.getSQLJSON("c2");
            assertEquals((float)1.0, sqljson.getFloat());

            sqljson = rs.getSQLJSON("c3");
            assertEquals( Float.MAX_VALUE, sqljson.getFloat());

            sqljson = rs.getSQLJSON("c4");
            assertEquals( Float.MIN_VALUE, sqljson.getFloat());

            sqljson = rs.getSQLJSON("c5");
            assertNull(sqljson);
            assertTrue(rs.wasNull());

        }

    }

    @Ignore("Known problem with maximal values.")
    @Test
    public void testSetDouble() throws Exception
    {
        SQLJSON sqljson = ((CBConnection)con).createSQLJSON();
        sqljson.setDouble(Double.MAX_VALUE);

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1, "val1");
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1, "val2");
            sqljson.setDouble(0);
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1, "val3");
            sqljson.setDouble(Double.MIN_VALUE);
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            try (Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select default from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals(Double.MAX_VALUE,sqljson1.getDouble());
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val2'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals(0.0,sqljson1.getDouble());
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val3'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals(Double.MIN_VALUE,sqljson1.getDouble());
                }

            }
        }
    }

    @Ignore("Known problem with maximal values.")
    @Test
    public void testGetDouble() throws Exception
    {
        String query = "SELECT 0 as c1, 1 as c2, " + Double.valueOf(Double.MAX_VALUE) + " as c3, " + Double.valueOf(Double.MIN_VALUE) + " as c4, null as c5";

        try (Statement stmt= con.createStatement())
        {
            CBResultSet rs = (CBResultSet) stmt.executeQuery(query);
            assertNotNull(rs);

            assertTrue(rs.next());


            SQLJSON sqljson = rs.getSQLJSON("c1");
            assertEquals(0.0,sqljson.getDouble());

            sqljson = rs.getSQLJSON("c2");
            assertEquals(1.0, sqljson.getDouble());

            sqljson = rs.getSQLJSON("c3");
            assertEquals( Double.MAX_VALUE, sqljson.getDouble());

            sqljson = rs.getSQLJSON("c4");
            assertEquals( Double.MIN_VALUE, sqljson.getDouble());

            sqljson = rs.getSQLJSON("c5");
            assertNull( sqljson);
            assertTrue(rs.wasNull());

        }
    }

    @Test
    public void testSetDate() throws Exception
    {
        Calendar calendar= Calendar.getInstance();
        SQLJSON sqljson = ((CBConnection)con).createSQLJSON();

        java.sql.Date date = new java.sql.Date(calendar.getTime().getTime());
        sqljson.setDate( date, null);

        Date val = sqljson.getDate(null);
        Calendar calendar1 = Calendar.getInstance();
        calendar1.setTime(val);

        assertEquals(calendar.get(Calendar.MONTH), calendar1.get(Calendar.MONTH));
        assertEquals(calendar.get(Calendar.DAY_OF_MONTH), calendar1.get(Calendar.DAY_OF_MONTH));
        assertEquals(calendar.get(Calendar.YEAR), calendar1.get(Calendar.YEAR));


    }

    @Test
    public void testGetDate() throws Exception
    {
        Calendar calendar= Calendar.getInstance(),
                 calendar1=Calendar.getInstance();

        try (Statement stmt = con.createStatement())
        {
            try (ResultSet rs = stmt.executeQuery("select now_str() as cur_time" ))
            {
                assertTrue(rs.next());
                SQLJSON sqljson = ((CBResultSet)rs).getSQLJSON("cur_time");
                assertNotNull(sqljson);

                Date date = sqljson.getDate(null);
                calendar1.setTime(date);

                //this may fail if run at midnight
                assertEquals(calendar.get(Calendar.MONTH), calendar1.get(Calendar.MONTH));
                assertEquals(calendar.get(Calendar.DAY_OF_MONTH), calendar1.get(Calendar.DAY_OF_MONTH));
                assertEquals(calendar.get(Calendar.YEAR), calendar1.get(Calendar.YEAR));

            }
        }
    }

    @Test
    public void testSetTime() throws Exception
    {

        Calendar calendar= Calendar.getInstance();
        SQLJSON sqljson = ((CBConnection)con).createSQLJSON();

        java.sql.Time time = new java.sql.Time(calendar.getTime().getTime());
        sqljson.setTime(time, null);

        Time val = sqljson.getTime(null);
        Calendar calendar1 = Calendar.getInstance();
        calendar1.setTime(val);

        assertEquals(calendar.get(Calendar.HOUR_OF_DAY), calendar1.get(Calendar.HOUR_OF_DAY));
        assertEquals(calendar.get(Calendar.MINUTE), calendar1.get(Calendar.MINUTE));
        assertEquals(calendar.get(Calendar.SECOND), calendar1.get(Calendar.SECOND));
    }

    @Test
    public void testGetTime() throws Exception
    {
        Calendar calendar1=Calendar.getInstance();

        try (Statement stmt = con.createStatement())
        {
            try (ResultSet rs = stmt.executeQuery("select '09:54:00' as cur_time" ))
            {
                assertTrue(rs.next());
                SQLJSON sqljson = ((CBResultSet)rs).getSQLJSON("cur_time");
                assertNotNull(sqljson);

                Time time = sqljson.getTime(null);
                calendar1.setTime(time);


                assertEquals(9, calendar1.get(Calendar.HOUR_OF_DAY));
                assertEquals(54, calendar1.get(Calendar.MINUTE));
                assertEquals(0, calendar1.get(Calendar.SECOND));

            }
        }
    }

    @Test
    public void testSetTimestamp() throws Exception
    {
        Calendar calendar= Calendar.getInstance();
        SQLJSON sqljson = ((CBConnection)con).createSQLJSON();

        Timestamp timestamp = new Timestamp(calendar.getTime().getTime());
        sqljson.setTimestamp(timestamp, null);

        Timestamp val = sqljson.getTimestamp(null);
        Calendar calendar1 = Calendar.getInstance();
        calendar1.setTime(val);

        assertEquals(calendar.get(Calendar.MONTH), calendar1.get(Calendar.MONTH));
        assertEquals(calendar.get(Calendar.DAY_OF_MONTH), calendar1.get(Calendar.DAY_OF_MONTH));
        assertEquals(calendar.get(Calendar.YEAR), calendar1.get(Calendar.YEAR));

        assertEquals(calendar.get(Calendar.HOUR_OF_DAY), calendar1.get(Calendar.HOUR_OF_DAY));
        assertEquals(calendar.get(Calendar.MINUTE), calendar1.get(Calendar.MINUTE));
        assertEquals(calendar.get(Calendar.SECOND), calendar1.get(Calendar.SECOND));

        assertEquals(calendar.get(Calendar.MILLISECOND),calendar.get(Calendar.MILLISECOND));
    }

    @Test
    public void testGetTimestamp() throws Exception
    {
        Calendar calendar1=Calendar.getInstance();

        try (Statement stmt = con.createStatement())
        {
            try (ResultSet rs = stmt.executeQuery("select '2015-09-14 09:54:00.12345' as cur_time" ))
            {
                assertTrue(rs.next());
                SQLJSON sqljson = ((CBResultSet)rs).getSQLJSON("cur_time");
                assertNotNull(sqljson);

                Timestamp timestamp = sqljson.getTimestamp(null);
                calendar1.setTime(timestamp);

                // month is 0 based
                assertEquals(8, calendar1.get(Calendar.MONTH));
                assertEquals(14, calendar1.get(Calendar.DAY_OF_MONTH));
                assertEquals(2015, calendar1.get(Calendar.YEAR));

                assertEquals(9, calendar1.get(Calendar.HOUR_OF_DAY));
                assertEquals(54, calendar1.get(Calendar.MINUTE));
                assertEquals(0, calendar1.get(Calendar.SECOND));

                assertEquals(0, calendar1.get(Calendar.SECOND));

                assertEquals(123450000, timestamp.getNanos());

            }
        }
    }
    @Test
    public void testGetMap() throws Exception
    {

        String query = "SELECT { 'a1': 'Object' } as c1, null as c2";

        Map map = new HashMap();
        map.put("a1","Object");

        try (Statement stmt= con.createStatement())
        {
            CBResultSet rs = (CBResultSet) stmt.executeQuery(query);
            assertNotNull(rs);

            assertTrue(rs.next());


            SQLJSON sqljson = rs.getSQLJSON("c1");
            assertEquals(map,sqljson.getMap());


            sqljson = rs.getSQLJSON("c2");
            assertNull(sqljson);
            assertTrue(rs.wasNull());

        }
    }

    @Test
    public void testSetMap() throws Exception
    {

        Map map = new HashMap();
        map.put("a1","Object");

        SQLJSON sqljson = ((CBConnection)con).createSQLJSON();
        sqljson.setMap(map);

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1, "val1");
            ((CBPreparedStatement) preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1, "val2");
            sqljson.setMap(null);
            ((CBPreparedStatement) preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());


            try (Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet) rs).getSQLJSON("default");
                    assertEquals(map, sqljson1.getMap());
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val2'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet) rs).getSQLJSON("default");
                    assertNull( sqljson1 );
                    assertTrue(rs.wasNull());
                }
            }
        }
    }
    //@Test
    public void testCompareTo() throws Exception
    {
        String query = "SELECT true as c1, false as c2, 0 as c3, 1 as c4, '' as c5, 'some' as c6, [1,2,3,5,8] as c7, [] as c8, { 'a1': 'Object' } as c9, {} as c10";

        try (Statement stmt= con.createStatement())
        {
            CBResultSet rs = (CBResultSet) stmt.executeQuery(query);
            assertNotNull(rs);

            assertTrue(rs.next());

            SQLJSON sqljson = rs.getSQLJSON("c1");
            SQLJSON sqljson1 = rs.getSQLJSON("c1");

            sqljson.compareTo(sqljson1);
        }

    }
    @Test
    public void testGetObject() throws Exception
    {
        String query = "SELECT true as c1, false as c2, 0 as c3, 1 as c4, '' as c5, " +
                "'some' as c6, [1,2,3,4,5,6] as c7, [] as c8, { 'a1': 'Object' } as c9, " +
                "{} as c10, '09:54:00' as time, '2015-09-14' as date, '2015-09-14 09:54:00.12345' as timestamp";

        try (Statement stmt= con.createStatement())
        {
            CBResultSet rs = (CBResultSet) stmt.executeQuery(query);
            assertNotNull(rs);

            assertTrue(rs.next());

            SQLJSON sqljson = rs.getSQLJSON("c1");
            assertTrue((boolean)sqljson.getObject());

            sqljson = rs.getSQLJSON("c2");
            assertFalse((boolean) sqljson.getObject());

            sqljson = rs.getSQLJSON("c3");
            assertEquals(0, sqljson.getObject());

            sqljson = rs.getSQLJSON("c4");
            assertEquals(1, sqljson.getObject());

            sqljson = rs.getSQLJSON("c5");
            assertEquals( "", sqljson.getObject());

            sqljson = rs.getSQLJSON("c6");
            assertEquals( "some", sqljson.getObject());

            sqljson = rs.getSQLJSON("c7");
            List returned = (List)sqljson.getObject();
            for (int i=0; i<6;i++)
                assertEquals(i+1,returned.get(i));

            sqljson = rs.getSQLJSON("c8");
            returned = (List)sqljson.getObject();

            assertTrue(returned.isEmpty());

            sqljson = rs.getSQLJSON("c9");
            Map map = (Map)sqljson.getObject();

            assertTrue(map.containsKey("a1"));
            assertEquals("Object", map.get("a1"));

            sqljson = rs.getSQLJSON("time");
            Time time = sqljson.getTime(null);

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(time);

            assertEquals(9, calendar.get(Calendar.HOUR_OF_DAY));
            assertEquals(54, calendar.get(Calendar.MINUTE));
            assertEquals(0, calendar.get(Calendar.SECOND));

            sqljson = rs.getSQLJSON("date");
            Date date = sqljson.getDate(null);

            calendar = Calendar.getInstance();
            calendar.setTime(date);

            assertEquals(2015, calendar.get(Calendar.YEAR));
            assertEquals(8, calendar.get(Calendar.MONTH));
            assertEquals(14, calendar.get(Calendar.DAY_OF_MONTH));

            sqljson = rs.getSQLJSON("timestamp");
            Timestamp timestamp = sqljson.getTimestamp(null);

            calendar = Calendar.getInstance();
            calendar.setTime(timestamp);

            assertEquals(9, calendar.get(Calendar.HOUR_OF_DAY));
            assertEquals(54, calendar.get(Calendar.MINUTE));
            assertEquals(0, calendar.get(Calendar.SECOND));

            assertEquals(2015, calendar.get(Calendar.YEAR));
            assertEquals(8, calendar.get(Calendar.MONTH));
            assertEquals(14, calendar.get(Calendar.DAY_OF_MONTH));

            assertEquals(123450000,timestamp.getNanos());
        }
    }

    @Test
    public void testSetObject() throws Exception
    {
        SQLJSON sqljson = ((CBConnection)con).createSQLJSON();

        sqljson.setObject(Boolean.TRUE);
        assertTrue((Boolean) sqljson.getObject());
        assertTrue(sqljson.getBoolean());
        assertEquals(sqljson.getJDBCType(),Types.BOOLEAN);

        sqljson.setObject(1);
        assertEquals(1, (long)sqljson.getObject());
        assertEquals(1, sqljson.getInt());
        assertEquals(sqljson.getJDBCType(),Types.NUMERIC);

        sqljson.setObject(1.0);
        assertEquals(1.0, (double)sqljson.getObject(),0);
        assertEquals(1.0, sqljson.getDouble(),0);
        assertEquals(sqljson.getJDBCType(),Types.NUMERIC);

        sqljson.setObject("string");
        assertEquals("string", (String)sqljson.getObject());
        assertEquals("string", sqljson.getString());
        assertEquals(Types.VARCHAR,sqljson.getJDBCType());

        int [] array = {1,2,3,4,5,6};
        sqljson.setObject(array);

        List <Integer>list =  (List <Integer>)sqljson.getObject() ;
        List <Integer>list1 =  (List <Integer>)sqljson.getArray()  ;

        for( int i=0; i< 6; i++)
        {
            assertEquals(i + 1, (int)list.get(i));
            assertEquals(i+1, (int)list1.get(i));
        }
        assertEquals(Types.ARRAY,sqljson.getJDBCType());


    }

    @Test
    public void testGetJDBCType() throws Exception
    {
        String query = "SELECT true as c1, false as c2, 0 as c3, 1 as c4, null as c5, " +
                "'some' as c6, [1,2,3,4,5,6] as c7, [] as c8, { 'a1': 'Object' } as c9, " +
                "{} as c10, '09:54:00' as time, '2015-09-14 09:54:00' as date, '2015-09-14 09:54:00.12345' as timestamp";

        try (Statement stmt= con.createStatement())
        {
            CBResultSet rs = (CBResultSet) stmt.executeQuery(query);
            assertNotNull(rs);

            assertTrue(rs.next());

            SQLJSON sqljson = rs.getSQLJSON("c1");
            assertEquals(sqljson.getJDBCType(), Types.BOOLEAN);

            sqljson = rs.getSQLJSON("c2");
            assertEquals(sqljson.getJDBCType(), Types.BOOLEAN);

            sqljson = rs.getSQLJSON("c3");
            assertEquals(sqljson.getJDBCType(), Types.NUMERIC);

            sqljson = rs.getSQLJSON("c4");
            assertEquals(sqljson.getJDBCType(), Types.NUMERIC);

            sqljson = rs.getSQLJSON("c5");
            assertNull(sqljson);

            sqljson = rs.getSQLJSON("c6");
            assertEquals(sqljson.getJDBCType(), Types.VARCHAR);

            sqljson = rs.getSQLJSON("c7");
            assertEquals(sqljson.getJDBCType(), Types.ARRAY);

            sqljson = rs.getSQLJSON("c8");
            assertEquals(sqljson.getJDBCType(), Types.ARRAY);

            sqljson = rs.getSQLJSON("c9");
            assertEquals(sqljson.getJDBCType(), Types.JAVA_OBJECT);

            sqljson = rs.getSQLJSON("time");
            assertEquals(sqljson.getJDBCType(), Types.VARCHAR);

            sqljson = rs.getSQLJSON("date");
            assertEquals(sqljson.getJDBCType(), Types.VARCHAR);

            sqljson = rs.getSQLJSON("timestamp");
            assertEquals(sqljson.getJDBCType(), Types.VARCHAR);

        }
    }
}
