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
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by davec on 2015-02-26.
 */
@RunWith(JUnit4.class)
public class StatementTest extends TestCase
{
    Connection con;

    @Before
    public void openConnection() throws Exception
    {
        con = DriverManager.getConnection(TestUtil.getURL(), TestUtil.getUser(), TestUtil.getPassword());
    }
    @After
    public void closeConnection() throws Exception
    {
        assertNotNull(con);
        con.close();
    }
    @Test
    public void createStatement() throws Exception
    {
        assertNotNull(con);
        Statement statement = con.createStatement();
        assertNotNull(statement);

    }
    @Test
    public void emptyResult() throws Exception
    {
        assertNotNull(con);
        Statement statement = con.createStatement();
        assertNotNull(statement);

        ResultSet rs = statement.executeQuery("select * from test1");
        assertFalse(rs.next());

    }
    @Test
    public void simpleSelect() throws Exception
    {
        assertNotNull(con);
        Statement statement = con.createStatement();
        assertNotNull(statement);

        ResultSet rs = statement.executeQuery("select 1");

        assertTrue(rs.next());
        assertEquals("1",rs.getString(1));

    }

    @Test
    public void simpleInsert() throws Exception
    {
        assertNotNull(con);
        Statement statement = con.createStatement();
        assertNotNull(statement);

        for (int i = 0; i++< 100;)
        {

            int inserted = statement.executeUpdate("INSERT INTO test1  (KEY, VALUE) VALUES ( 'K" + i +"'," + i +")");
            assertEquals(1, inserted);
        }

        ResultSet resultSet = statement.executeQuery("select count(1) as test_count from test1");
        assertTrue(resultSet.next());
        assertEquals(100,resultSet.getInt("test_count"));

        resultSet = statement.executeQuery("select test1 from test1 order by test1");
        for (int i=0; resultSet.next(); i++)
        {
            assertEquals(i+1, resultSet.getInt(1));
        }
        statement.executeUpdate("delete from test1");

    }
}
