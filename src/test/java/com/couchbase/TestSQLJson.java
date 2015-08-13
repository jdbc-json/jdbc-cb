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
import com.couchbase.json.SQLJSON;
import junit.framework.TestCase;
import org.boon.json.JsonFactory;
import org.boon.json.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

/**
 * Created by davec on 2015-07-10.
 */
@RunWith(JUnit4.class)
public class TestSQLJson extends TestCase
{

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    //todo fill in all of these tests
    Connection con;

    @Before
    public void openConnection() throws Exception
    {
        con = DriverManager.getConnection(TestUtil.getURL(), TestUtil.getUser(), TestUtil.getPassword());
        assertNotNull(con);
        con.createStatement().executeUpdate("delete from default");
        System.out.print("connection opened");
    }

    @After
    public void closeConnection() throws Exception
    {
        assertNotNull(con);

        try(Statement statement = con.createStatement())
        {
            statement.executeUpdate("delete from default");
        }

        con.close();
    }

    @Test
    public void getSqlJson() throws Exception
    {
        String query = "SELECT * FROM customer limit 10";

        try (Statement stmt= con.createStatement())
        {
            CBResultSet rs = (CBResultSet)stmt.executeQuery(query);
            assertNotNull(rs);

            while (rs.next())
            {
                SQLJSON sqljson = rs.getSQLJSON(1);
                Map map = sqljson.parse();
                assertNotNull(map);
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
    public void testFree() throws Exception
    {

    }

    @Test
    public void testGetBinaryStream() throws Exception
    {

    }

    @Test
    public void testGetCharacterStream() throws Exception
    {

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
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertEquals("a string", sqljson1.getString());
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val2'"))
                {
                    assertTrue(rs.next());

                    SQLJSON sqljson1 = ((CBResultSet)rs).getSQLJSON("default");
                    assertNull(sqljson1.getString());
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
            try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
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
    public void testSetBinaryStream() throws Exception
    {
        //todo implement
    }

    @Test
    public void testSetCharacterStream() throws Exception
    {
        //todo implement
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
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
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
            assertEquals((byte) 0, sqljson.getByte());
            assertTrue(sqljson.isNull());



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
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
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
            assertEquals( 0, sqljson.getShort());
            assertTrue(sqljson.isNull());

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
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
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
            assertEquals( 0, sqljson.getInt());
            assertTrue(sqljson.isNull());

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
            sqljson.setInt((int) 0);
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1, "val3");
            sqljson.setInt(Integer.MIN_VALUE);
            ((CBPreparedStatement)preparedStatement).setSQLJSON(2, sqljson);

            assertEquals(1, preparedStatement.executeUpdate());


            try (Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
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
            assertEquals(0, sqljson.getInt());
            assertTrue(sqljson.isNull());

        }
    }

    @Test
    public void testSetLong() throws Exception
    {
        SQLJSON sqljson = ((CBConnection)con).createSQLJSON();
        sqljson.setLong((long) Long.MAX_VALUE);

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
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
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
            assertNull(sqljson.getBigDecimal());
            assertTrue(sqljson.isNull());

        }
    }

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
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
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
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
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
            assertEquals((float)0.0, sqljson.getFloat());
            assertTrue(sqljson.isNull());

        }

    }

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
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
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
            assertEquals( 0.0, sqljson.getDouble());
            assertTrue(sqljson.isNull());

        }
    }

    @Test
    public void testSetBytes() throws Exception
    {

    }

    @Test
    public void testGetBytes() throws Exception
    {

    }

    @Test
    public void testSetDate() throws Exception
    {

    }

    @Test
    public void testGetDate() throws Exception
    {

    }

    @Test
    public void testSetTime() throws Exception
    {

    }

    @Test
    public void testGetTime() throws Exception
    {

    }

    @Test
    public void testSetTimestamp() throws Exception
    {

    }

    @Test
    public void testGetTimestamp() throws Exception
    {

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
            assertNull(sqljson.getMap());

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
                    assertNull( sqljson1.getMap());
                }
            }
        }
    }

    @Test
    public void testGetObject() throws Exception
    {

    }

    @Test
    public void testSetObject() throws Exception
    {

    }

    @Test
    public void testGetJDBCType() throws Exception
    {

    }

    @Test
    public void testParse() throws Exception
    {

    }

    @Test
    public void testParse1() throws Exception
    {

    }
}
