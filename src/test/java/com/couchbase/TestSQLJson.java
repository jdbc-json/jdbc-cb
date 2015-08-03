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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.*;
import java.util.Map;

/**
 * Created by davec on 2015-07-10.
 */
@RunWith(JUnit4.class)
public class TestSQLJson extends TestCase
{
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

        String query = "insert into default (key,value) values (?,?)";

        SQLJSON sqljson = ((CBConnection)con).createSQLJSON();
        sqljson.setString( json );
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


        }
    }
    @Test
    public void testSetString() throws Exception
    {

    }

    @Test
    public void testSetBinaryStream() throws Exception
    {

    }

    @Test
    public void testSetCharacterStream() throws Exception
    {

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
        sqljson.setByte((byte)1);

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1, "val1");
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
            }
        }
    }

    @Test
    public void testGetByte() throws Exception
    {

    }

    @Test
    public void testSetByte() throws Exception
    {

    }

    @Test
    public void testGetShort() throws Exception
    {

    }

    @Test
    public void testSetShort() throws Exception
    {

    }

    @Test
    public void testGetInt() throws Exception
    {

    }

    @Test
    public void testSetInt() throws Exception
    {

    }

    @Test
    public void testGetLong() throws Exception
    {

    }

    @Test
    public void testSetLong() throws Exception
    {

    }

    @Test
    public void testGetBigDecimal() throws Exception
    {

    }

    @Test
    public void testSetBigDecimal() throws Exception
    {

    }

    @Test
    public void testGetMap() throws Exception
    {

    }

    @Test
    public void testSetMap() throws Exception
    {

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
