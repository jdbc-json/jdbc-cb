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
import org.hamcrest.core.IsEqual;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.After;
import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import com.couchbase.jdbc.CBArray;
import com.couchbase.jdbc.CBResultSet;

import java.sql.*;
import java.sql.Date;
import java.util.*;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class PreparedStatementTest extends CouchBaseTestCase
{

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
 
    @After
	public void cleanupBucket() throws Exception
	{
		JDBCTestUtils.deleteDataFromBucket("default");
	}
    
    @Test
    public void createStatement() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("select 1"))
        {
            assertNotNull(preparedStatement);

            preparedStatement.close();
        }
    }

    @Test
    public void emptyResult() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("select * from default where nonexistentfield = 'does not exist'"))
        {
            assertNotNull(preparedStatement);

            ResultSet rs = preparedStatement.executeQuery();
            assertFalse(rs.next());
        }

    }

    @Test
    public void testExecuteQuery() throws Exception
    {
        try (PreparedStatement preparedStatement = con.prepareStatement("INSERT INTO default  (KEY, VALUE) VALUES ( ?, ?)"))
        {
            assertNotNull(preparedStatement);

            for (int i = 0; i++< 100;)
            {
                preparedStatement.setString(1, "K"+i);
                preparedStatement.setInt(2,i);

                int inserted = preparedStatement.executeUpdate();
                assertEquals(1, inserted);
            }

        }

        try( PreparedStatement preparedStatement = con.prepareStatement("SELECT COUNT(*) AS test1_count FROM default"))
        {
            assertNotNull(preparedStatement);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals( 100, rs.getInt(1) );
        }

        try ( PreparedStatement preparedStatement = con.prepareStatement( "SELECT default FROM default WHERE default >= ? order by default"))
        {
            assertNotNull(preparedStatement);
            preparedStatement.setInt(1,50);
            ResultSet rs = preparedStatement.executeQuery();
            for (int i=0; i< 50;i++)
            {
                assertTrue(rs.next());
                assertEquals(50+i, rs.getInt(1));
            }

        }

    }

    @Test
    public void testN1QLSpecific() throws Exception
    {
        String jsonString = "{ \"age\": 56, \"children\": [ { \"age\": 17, \"fname\": \"Abama\", \"gender\": \"m\"}," +
         "{ \"age\": 21, \"fname\": \"Bebama\", \"gender\": \"m\" } ]," +
         "\"email\": \"ian@gmail.com\", \"fname\": \"Ian\" }";

        Object jsonObject = JsonFactory.fromJson(new StringReader(jsonString));

        String jsonString2 = "{ \"age\": 56," +
                "\"email\": \"ian@gmail.com\", \"fname\": \"Ian\" }";
        Object jsonObject2 = JsonFactory.fromJson(new StringReader(jsonString2));

        try (PreparedStatement preparedStatement = con.prepareStatement("insert into default (key,value) values(?,?)"))
        {
            assertNotNull(preparedStatement);

            preparedStatement.setString(1, "employee");
            preparedStatement.setObject(2, jsonObject);

            assertEquals(1,preparedStatement.executeUpdate());


        }
        try (Statement statement = con.createStatement()) {
            assertNotNull(statement);
            ResultSet resultSet = statement.executeQuery("SELECT emp.children[0].fname AS cname\n" +
                    "FROM default emp\n" +
                    "WHERE children is not NULL\n");

            assertTrue(resultSet.next());
            assertEquals("Abama",resultSet.getString(1));

        }
        try(Statement statement = con.createStatement()){
            statement.executeUpdate("delete from default");
        }

        try (PreparedStatement preparedStatement = con.prepareStatement("insert into default (key,value) values(?,?)"))
        {
            assertNotNull(preparedStatement);

            preparedStatement.setString(1, "employees");
            preparedStatement.setObject(2,jsonObject2);

            assertEquals(1,preparedStatement.executeUpdate());


        }
        try (Statement statement = con.createStatement()) {
            assertNotNull(statement);
            ResultSet resultSet = statement.executeQuery("SELECT emp.children[0].fname AS cname\n" +
                    "FROM default emp\n" +
                    "WHERE children is not NULL\n");

            assertFalse(resultSet.next());

        }
        try(Statement statement = con.createStatement()){
            statement.executeUpdate("delete from default");
        }


    }
    @Test
    public void schemaLessTest() throws Exception
    {

        String name1 = "{\"name\":\"Travel Route1\", \"cities\": [ \"C1\", \"C2\", \"C3\" ],  " +
                "\"type\": \"route\" }";
        String name2 = "{\"name\":\"First Town\", \"type\": \"city\"}";
        String name3 = "{\"name\":\"Second Stop\", \"type\": \"city\"}";
        String name4 = "{\"name\":\"Destination\", \"type\": \"city\"}";

        try (PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            Map <String, String> jsonObject = (Map <String, String>)JsonFactory.fromJson(new StringReader(name1));
            preparedStatement.setString(1,"name");
            preparedStatement.setString(2, jsonObject.get("name"));

            assertEquals(1, preparedStatement.executeUpdate());

            jsonObject = (Map <String, String>)JsonFactory.fromJson(new StringReader(name2));
            preparedStatement.setString(1,"name1");
            preparedStatement.setString(2, jsonObject.get("name"));

            assertEquals(1, preparedStatement.executeUpdate());

            jsonObject = (Map <String, String>)JsonFactory.fromJson(new StringReader(name3));
            preparedStatement.setString(1,"name2");
            preparedStatement.setString(2, jsonObject.get("name"));

            assertEquals(1, preparedStatement.executeUpdate());

            jsonObject = (Map <String, String>)JsonFactory.fromJson(new StringReader(name4));
            preparedStatement.setString(1,"name3");
            preparedStatement.setString(2, jsonObject.get("name"));

            assertEquals(1, preparedStatement.executeUpdate());
        }
        /*
        try(Statement statement = con.createStatement())
        {
            ResultSet rs = statement.executeQuery("SELECT r.name as route_name, c as route_cities " +
                                                    "FROM default r NEST travel c ON KEYS r.cities" +
                                                    " WHERE r.name = \"Travel Route1\"");
            assertTrue(rs.next());
        }
        */

    }
    @Test
    public void testExecute() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1,"byte1");
            preparedStatement.setByte(2, (byte)1);

            assertFalse(preparedStatement.execute());

            preparedStatement.setString(1,"byte2");
            preparedStatement.setByte(2, (byte)255);

            assertFalse(preparedStatement.execute());

            preparedStatement.setString(1,"byte3");
            preparedStatement.setNull(2, Types.INTEGER, "byte");

            assertFalse(preparedStatement.execute());

            try(PreparedStatement preparedStatement1 = con.prepareStatement("select * from default where meta(default).id='byte1'"))
            {

                assertTrue(preparedStatement1.execute());
                try (ResultSet rs = preparedStatement1.getResultSet())
                {
                    assertTrue(rs.next());
                    assertEquals((byte)1, rs.getByte("default"));
                }

                assertTrue(preparedStatement1.execute("select * from default where meta(default).id='byte2'"));
                try (ResultSet rs = preparedStatement1.getResultSet())
                {
                    assertTrue(rs.next());
                    assertEquals((byte) 255, rs.getByte("default"));
                    assertFalse(rs.wasNull());

                }
                assertTrue(preparedStatement1.execute("select * from default where meta(default).id='byte3'"));
                try (ResultSet rs = preparedStatement1.getResultSet())
                {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getByte("default"));
                    assertTrue(rs.wasNull());
                }
            }

        }
    }

    @Test
    public void testPreparedReturning() throws Exception
    {
        String jsonString = "{\"name\":\"Alex Baldwin\", \"type\":\"contact\"}";
        Map jsonObject = new HashMap();
        jsonObject.put("name", "Alex Baldwin");
        jsonObject.put("type", "contact");

        String []columns = {"default"};
    /*
        [2015-08-11, 1:57:10 PM] Colm McHugh:INSERT INTO contacts (KEY, VALUE)
        VALUES ("baldwin", {"name":"Alex Baldwin", "type":"contact"})
        RETURNING contacts
        [2015-08-11, 1:57:37 PM] Colm McHugh: UPDATE contacts
        USE KEYS "baldwin"
        SET children = ARRAY_APPEND(children, { "name": "Julie", "age": 3 } )
        RETURNING contacts
        [2015-08-11, 1:57:54 PM] Colm McHugh: DELETE FROM contacts c
        USE KEYS "baldwin"
        RETURNING contacts

        */
        try (PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)",columns))
        {
            preparedStatement.setString(1,"baldwin");
            preparedStatement.setObject(2,jsonObject);

            boolean hasResults = preparedStatement.execute();
            if ( hasResults )
            {
                try (ResultSet rs = preparedStatement.getResultSet() )
                {
                    assertTrue(rs.next());
                    SQLJSON sqljson = ((CBResultSet)rs).getSQLJSON(1);
                    String result = sqljson.getString();
                    assertNotNull(result);
                }
            }
        }
    }
    @Test
    public void testSetByte() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1,"byte1");
            preparedStatement.setByte(2, (byte)1);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1,"byte2");
            preparedStatement.setByte(2, (byte)255);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1,"byte3");
            preparedStatement.setNull(2, Types.INTEGER, "byte");

            assertEquals(1, preparedStatement.executeUpdate());

            try(Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='byte1'"))
                {
                    assertTrue(rs.next());
                    assertEquals((byte)1, rs.getByte("default"));
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='byte2'"))
                {
                    assertTrue(rs.next());
                    assertEquals((byte) 255, rs.getByte("default"));
                    assertFalse(rs.wasNull());

                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='byte3'"))
                {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getByte("default"));
                    assertTrue(rs.wasNull());
                }
            }

        }
    }

    @Test
    public void testSetBytes() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1,"bytes1");
            preparedStatement.setBytes(2, "Hello World".getBytes());

            assertEquals(1, preparedStatement.executeUpdate());


            preparedStatement.setString(1,"bytes2");
            preparedStatement.setNull(2, Types.VARCHAR, "bytes");

            assertEquals(1, preparedStatement.executeUpdate());

            try(Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='bytes1'"))
                {
                    assertTrue(rs.next());
                    byte [] bytes = rs.getBytes(1);
                    assertThat("Hello World".getBytes(), IsEqual.equalTo(bytes));
                    bytes = rs.getBytes("default");
                    assertThat("Hello World".getBytes(), IsEqual.equalTo(bytes));

                }

                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='bytes2'"))
                {
                    assertTrue(rs.next());
                    assertNull(rs.getBytes("default"));
                    assertTrue(rs.wasNull());
                }
            }

        }
    }

    @Test
    public void testSetShort() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1,"val1");
            preparedStatement.setShort(2, (short)1);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1,"val2");
            preparedStatement.setShort(2, (short)-1);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1,"val3");
            preparedStatement.setNull(2, Types.INTEGER, "integer");

            assertEquals(1, preparedStatement.executeUpdate());

            try(Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getShort("default"));
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val2'"))
                {
                    assertTrue(rs.next());
                    assertEquals(-1, rs.getShort("default"));
                    assertFalse(rs.wasNull());

                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val3'"))
                {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getShort("default"));
                    assertTrue(rs.wasNull());
                }
            }

        }
    }

    @Test
    public void testSetInteger() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1,"val1");
            preparedStatement.setInt(2, 1);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1,"val2");
            preparedStatement.setInt(2, -1);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1,"val3");
            preparedStatement.setNull(2, Types.INTEGER, "integer");

            assertEquals(1, preparedStatement.executeUpdate());

            try(Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt("default"));
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val2'"))
                {
                    assertTrue(rs.next());
                    assertEquals(-1, rs.getInt("default"));
                    assertFalse(rs.wasNull());

                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val3'"))
                {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt("default"));
                    assertTrue(rs.wasNull());
                }
            }

        }
    }
    @Test
    public void testSetLong() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1,"val1");
            preparedStatement.setLong(2, 1);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1,"val2");
            preparedStatement.setLong(2, -1);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1,"val3");
            preparedStatement.setNull(2, Types.INTEGER, "long");

            assertEquals(1, preparedStatement.executeUpdate());

            try(Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());

                    assertEquals(1, rs.getLong("default"));
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val2'"))
                {
                    assertTrue(rs.next());
                    assertEquals(-1, rs.getLong("default"));
                    assertFalse(rs.wasNull());
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val3'"))
                {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getLong("default"));
                    assertTrue(rs.wasNull());
                }
            }

        }
    }

    @Test
    public void testSetBigDecimal() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1,"val1");
            preparedStatement.setBigDecimal(2, BigDecimal.ONE);

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1,"val2");
            preparedStatement.setBigDecimal(2, BigDecimal.valueOf(-1));

            assertEquals(1, preparedStatement.executeUpdate());

            preparedStatement.setString(1,"val3");
            preparedStatement.setNull(2, Types.DECIMAL, "bigdecimal");

            assertEquals(1, preparedStatement.executeUpdate());

            try(Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());
                    assertEquals(BigDecimal.ONE, rs.getBigDecimal("default"));
                    assertEquals(BigDecimal.ONE, rs.getBigDecimal(1));
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val2'"))
                {
                    assertTrue(rs.next());
                    assertEquals(BigDecimal.valueOf(-1), rs.getBigDecimal("default"));
                    assertFalse(rs.wasNull());
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val3'"))
                {
                    assertTrue(rs.next());
                    assertNull(rs.getBigDecimal("default"));
                    assertTrue(rs.wasNull());
                }
            }

        }
    }
    @Test
    public void testSetSimpleArray() throws Exception
    {
        Object [] values = {"1", "2"};
        Array array = con.createArrayOf("int", values);

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1,"val1");
            preparedStatement.setArray(2, array);

            assertEquals(1, preparedStatement.executeUpdate());


            preparedStatement.setString(1,"val3");
            preparedStatement.setNull(2, Types.ARRAY, "array");

            assertEquals(1, preparedStatement.executeUpdate());

            try(Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());
                    Assert.assertThat(array, IsEqual.equalTo(rs.getArray("default")));
                    assertEquals(array, rs.getArray(1));
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val3'"))
                {
                    assertTrue(rs.next());
                    assertNull(rs.getArray("default"));
                    assertTrue(rs.wasNull());
                }
            }

        }
    }
    @Test
    public void testSetUserArray() throws Exception
    {
        List arrayList = new ArrayList();
        arrayList.add(new TestUser("dave", "cramer", 54, true));
        arrayList.add(new TestUser("joe", "shmo", 15, false));
        arrayList.add(new TestUser("sue", "sandy", 30, true));

        Array array = con.createArrayOf("TestUser", arrayList.toArray());

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1,"val1");
            preparedStatement.setArray(2, array);

            assertEquals(1, preparedStatement.executeUpdate());


            preparedStatement.setString(1,"val3");
            preparedStatement.setNull(2, Types.ARRAY, "array");

            assertEquals(1, preparedStatement.executeUpdate());

            try(Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());
                    List testArray = JsonFactory.fromJsonArray(((CBArray) rs.getArray("default")).getJsonArray(), TestUser.class);

                    Assert.assertThat(arrayList,IsEqual.equalTo(testArray));
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val3'"))
                {
                    assertTrue(rs.next());
                    assertNull( rs.getArray("default"));
                    assertTrue( rs.wasNull() );
                }
            }

        }
    }
    @Test
    public void testSetDate() throws Exception
    {
        Calendar cal = Calendar.getInstance();


        cal.set(2015,Calendar.JANUARY,31,23,59,59);
        Calendar cal2 = Calendar.getInstance();

        Date date = new Date(cal.getTime().getTime());

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1,"val1");
            preparedStatement.setDate(2, date);

            assertEquals(1, preparedStatement.executeUpdate());


            preparedStatement.setString(1,"val3");
            preparedStatement.setNull(2, Types.DATE, "date");

            assertEquals(1, preparedStatement.executeUpdate());

            try(Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());
                    Date date1 = rs.getDate("default");
                    cal2.setTime(date1);

                    assertEquals(cal.get(Calendar.YEAR), cal2.get(Calendar.YEAR));
                    assertEquals(cal.get(Calendar.MONTH), cal2.get(Calendar.MONTH));
                    assertEquals(cal.get(Calendar.DAY_OF_MONTH),cal2.get(Calendar.DAY_OF_MONTH));

                    date1 = rs.getDate(1);
                    cal2.setTime(date1);

                    assertEquals(cal.get(Calendar.YEAR), cal2.get(Calendar.YEAR));
                    assertEquals(cal.get(Calendar.MONTH), cal2.get(Calendar.MONTH));
                    assertEquals(cal.get(Calendar.DAY_OF_MONTH),cal2.get(Calendar.DAY_OF_MONTH));

                    Calendar differentTimezone = Calendar.getInstance();

                    differentTimezone.setTimeZone(TimeZone.getTimeZone("GMT"));
                    date1 = rs.getDate(1, differentTimezone);

                    cal2.setTime(date1);

                    int offset  = cal2.getTimeZone().getRawOffset()/1000/60/60;
                    cal2.add(Calendar.HOUR, offset);

                    assertEquals(cal.get(Calendar.YEAR), cal2.get(Calendar.YEAR));
                    assertEquals(cal.get(Calendar.MONTH), cal2.get(Calendar.MONTH));
                    assertEquals(cal.get(Calendar.DAY_OF_MONTH),cal2.get(Calendar.DAY_OF_MONTH));

                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val3'"))
                {
                    assertTrue(rs.next());
                    assertNull( rs.getDate(1));
                }
            }

        }
    }
    @Test
    public void testSetTime() throws Exception
    {
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("America/Toronto"));
        Calendar cal2 = Calendar.getInstance();
        cal2.setTimeZone(TimeZone.getTimeZone("America/Toronto"));

        Time time = new Time(cal.getTime().getTime());

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1,"val1");
            preparedStatement.setTime(2, time);

            assertEquals(1, preparedStatement.executeUpdate());


            preparedStatement.setString(1,"val3");
            preparedStatement.setNull(2, Types.TIME, "time");

            assertEquals(1, preparedStatement.executeUpdate());

            try(Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());
                    Time time1 = rs.getTime("default");
                    cal2.setTime(time1);

                    assertEquals(cal.get(Calendar.HOUR_OF_DAY),cal2.get(Calendar.HOUR_OF_DAY));
                    assertEquals(cal.get(Calendar.MINUTE), cal2.get(Calendar.MINUTE));
                    assertEquals(cal.get(Calendar.SECOND),cal2.get(Calendar.SECOND));

                    time1 = rs.getTime(1);
                    cal2.setTime(time1);

                    assertEquals(cal.get(Calendar.HOUR_OF_DAY),cal2.get(Calendar.HOUR_OF_DAY));
                    assertEquals(cal.get(Calendar.MINUTE), cal2.get(Calendar.MINUTE));
                    assertEquals(cal.get(Calendar.SECOND),cal2.get(Calendar.SECOND));
                }
                
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val3'"))
                {
                    assertTrue(rs.next());
                    assertNull( rs.getTime(1));
                }
            }

        }
    }
    @Test
    public void testSetTimeStamp() throws Exception
    {
        Calendar cal = Calendar.getInstance();
        Calendar cal2 = Calendar.getInstance();

        Timestamp timeStamp = new Timestamp(cal.getTime().getTime());

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1,"val1");
            preparedStatement.setTimestamp(2, timeStamp);

            assertEquals(1, preparedStatement.executeUpdate());


            preparedStatement.setString(1,"val3");
            preparedStatement.setNull(2, Types.TIMESTAMP, "timeStamp");

            assertEquals(1, preparedStatement.executeUpdate());

            try(Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
                {
                    assertTrue(rs.next());
                    Timestamp timeStamp1 = rs.getTimestamp("default");
                    cal2.setTime(timeStamp1);

                    assertEquals(cal.get(Calendar.YEAR), cal2.get(Calendar.YEAR));
                    assertEquals(cal.get(Calendar.MONTH), cal2.get(Calendar.MONTH));
                    assertEquals(cal.get(Calendar.DAY_OF_MONTH),cal2.get(Calendar.DAY_OF_MONTH));

                    assertEquals(cal.get(Calendar.HOUR),cal2.get(Calendar.HOUR));
                    assertEquals(cal.get(Calendar.MINUTE), cal2.get(Calendar.MINUTE));
                    assertEquals(cal.get(Calendar.SECOND),cal2.get(Calendar.SECOND));

                    assertEquals(timeStamp.getNanos(), timeStamp1.getNanos());

                    timeStamp1 = rs.getTimestamp(1);
                    cal2.setTime(timeStamp1);

                    assertEquals(cal.get(Calendar.YEAR), cal2.get(Calendar.YEAR));
                    assertEquals(cal.get(Calendar.MONTH), cal2.get(Calendar.MONTH));
                    assertEquals(cal.get(Calendar.DAY_OF_MONTH),cal2.get(Calendar.DAY_OF_MONTH));

                    assertEquals(cal.get(Calendar.HOUR),cal2.get(Calendar.HOUR));
                    assertEquals(cal.get(Calendar.MINUTE), cal2.get(Calendar.MINUTE));
                    assertEquals(cal.get(Calendar.SECOND),cal2.get(Calendar.SECOND));

                    assertEquals(cal.get(Calendar.MILLISECOND),cal2.get(Calendar.MILLISECOND));
                }
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val3'"))
                {
                    assertTrue(rs.next());
                    assertNull( rs.getTimestamp(1));
                }
            }

        }
    }

    @Test
    public void setObject() throws Exception
    {
        //TODO implement, specifically string, map, list,etc
    }
    @Test
    public void clobNotImplemented() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("com.couchbase.jdbc.CBPreparedStatement.setClob");
            preparedStatement.setClob(1,(Clob)null);

        }
    }
    @Test
    public void clob1NotImplemented() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("com.couchbase.jdbc.CBPreparedStatement.setClob");
            preparedStatement.setClob(1,null, 0);

        }
    }
    @Test
    public void clob2NotImplemented() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("com.couchbase.jdbc.CBPreparedStatement.setClob");

            preparedStatement.setClob(1,(Reader)null);

        }
    }
    @Test
    public void nClobNotImplemented() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("com.couchbase.jdbc.CBPreparedStatement.setNClob");

            preparedStatement.setNClob(1, (NClob) null);

        }
    }
    @Test
    public void nClob1NotImplemented() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("com.couchbase.jdbc.CBPreparedStatement.setNClob");

            preparedStatement.setNClob(1, null, 0);

        }
    }
    @Test
    public void nClob2NotImplemented() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("com.couchbase.jdbc.CBPreparedStatement.setNClob");
            preparedStatement.setNClob(1, (Reader) null);

        }
    }

    @Test
    public void blobNotImplemented() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("com.couchbase.jdbc.CBPreparedStatement.setBlob");
            preparedStatement.setBlob(1,(Blob)null);

        }
    }
    @Test
    public void blob1NotImplemented() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("com.couchbase.jdbc.CBPreparedStatement.setBlob");

            preparedStatement.setBlob(1,null,0);

        }
    }
    @Test
    public void blob2NotImplemented() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("com.couchbase.jdbc.CBPreparedStatement.setBlob");
            preparedStatement.setBlob(1,(InputStream)null);

        }
    }

    @Test
    public void rowIdNotImplemented() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("com.couchbase.jdbc.CBPreparedStatement.setRowId");
            preparedStatement.setRowId(1,null);

        }
    }
    @Test
    public void refNotImplemented() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("com.couchbase.jdbc.CBPreparedStatement.setRef");
            preparedStatement.setRef(1, null);

        }
    }

   @Test
    public void getPreparedMetaData() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {

            ParameterMetaData pmd = preparedStatement.getParameterMetaData();
            assertNotNull(pmd);

        }
    }
    @Test
    public void getResultMetaData() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {

            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("com.couchbase.jdbc.CBPreparedStatement.getMetaData");
            ResultSetMetaData rsmd = preparedStatement.getMetaData();

        }
    }

    @Test
    public void testSetAsciiStream() throws Exception
    {
        String testString = "hello world";
        byte []bytes = testString.getBytes(StandardCharsets.US_ASCII);
        InputStream stream = new ByteArrayInputStream(bytes);

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)")) {
            preparedStatement.setString(1, "val1");
            preparedStatement.setAsciiStream(2, stream,bytes.length);
            assertEquals(1, preparedStatement.executeUpdate());

        }

        try (Statement statement = con.createStatement())
        {
            try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
            {
                assertTrue(rs.next());
                InputStream is = rs.getAsciiStream(1);
                char []buffer = new char[256]; // plenty big enough
                final StringBuilder out = new StringBuilder();
                try (Reader in = new InputStreamReader(is, StandardCharsets.US_ASCII)) {
                    for (;;) {
                        int rsz = in.read(buffer, 0, buffer.length);
                        if (rsz < 0)
                            break;
                        out.append(buffer, 0, rsz);
                    }
                }
                catch (UnsupportedEncodingException ex) {
                    /* ... */
                }
                catch (IOException ex) {
                    /* ... */
                }
                assertTrue(out.toString().equals(testString));
            }
        }

    }

    @Test
    public void testSetUnicodeStream() throws Exception
    {
        String testString = "hello world";
        byte []bytes = testString.getBytes(StandardCharsets.UTF_8);
        InputStream stream = new ByteArrayInputStream(bytes);

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)")) {
            preparedStatement.setString(1, "val1");
            preparedStatement.setUnicodeStream(2, stream, bytes.length);
            assertEquals(1, preparedStatement.executeUpdate());

        }

        try (Statement statement = con.createStatement())
        {
            try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
            {
                assertTrue(rs.next());
                InputStream is = rs.getAsciiStream(1);
                char []buffer = new char[256]; // plenty big enough
                final StringBuilder out = new StringBuilder();
                try (Reader in = new InputStreamReader(is, StandardCharsets.UTF_8)) {
                    for (;;) {
                        int rsz = in.read(buffer, 0, buffer.length);
                        if (rsz < 0)
                            break;
                        out.append(buffer, 0, rsz);
                    }
                }
                catch (UnsupportedEncodingException ex) {
                   /* ... */
                }
                catch (IOException ex) {
                   /* ... */
                }
                assertTrue(out.toString().equals(testString));
            }
        }

    }

    @Test
    public void testSetCharacterStream() throws Exception
    {

        String testString = "hello world";

        byte []bytes = testString.getBytes(StandardCharsets.UTF_8);
        InputStream stream = new ByteArrayInputStream(bytes);

        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)")) {
            preparedStatement.setString(1, "val1");
            preparedStatement.setCharacterStream(2, new StringReader(testString),testString.length());
            assertEquals(1, preparedStatement.executeUpdate());

        }

        try (Statement statement = con.createStatement())
        {
            try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='val1'"))
            {
                assertTrue(rs.next());
                Reader in = rs.getCharacterStream(1);
                char []buffer = new char[256]; // plenty big enough
                final StringBuilder out = new StringBuilder();
                for (;;) {
                    int rsz = in.read(buffer, 0, buffer.length);
                    if (rsz < 0)
                        break;
                    out.append(buffer, 0, rsz);
                }
                assertTrue(out.toString().equals(testString));
            }
        }

    }

    @Test
    public void testSetBinaryStream() throws Exception
    {
        byte [] bytes = new byte[10];
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {

            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("com.couchbase.jdbc.CBPreparedStatement.setBinaryStream");
            preparedStatement.setBinaryStream(2, new ByteArrayInputStream(bytes));

        }
    }

    @Test
    public void testSetCharacterStream1() throws Exception
    {
        String foo="blah";
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {

            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("com.couchbase.jdbc.CBPreparedStatement.setCharacterStream");
            preparedStatement.setCharacterStream(2, new StringReader(foo));

        }

    }

    @Test
    public void testSetAsciiStream1() throws Exception
    {
        byte [] bytes = new byte[10];
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {

            expectedException.expect(SQLFeatureNotSupportedException.class);
            expectedException.expectMessage("com.couchbase.jdbc.CBPreparedStatement.setAsciiStream");
            preparedStatement.setAsciiStream(2, new ByteArrayInputStream(bytes));

        }

    }


}