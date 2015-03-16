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
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;

public class PreparedStatementTest
{

    Connection con;

    @Before
    public void openConnection() throws Exception
    {
        con = DriverManager.getConnection(TestUtil.getURL(), TestUtil.getUser(), TestUtil.getPassword());
        assertNotNull(con);
    }

    @After
    public void closeConnection() throws Exception
    {
        assertNotNull(con);
        con.createStatement().executeUpdate("delete from test1");
        con.close();
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

        try(PreparedStatement preparedStatement = con.prepareStatement("select * from test1"))
        {
            assertNotNull(preparedStatement);

            ResultSet rs = preparedStatement.executeQuery();
            assertFalse(rs.next());

        }

    }

    @Test
    public void testExecuteQuery() throws Exception
    {
        try (PreparedStatement preparedStatement = con.prepareStatement("INSERT INTO test1  (KEY, VALUE) VALUES ( ?, ?)"))
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

        try( PreparedStatement preparedStatement = con.prepareStatement("SELECT COUNT(*) AS test1_count FROM test1"))
        {
            assertNotNull(preparedStatement);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals( 100, rs.getInt(1) );
        }

        try ( PreparedStatement preparedStatement = con.prepareStatement( "SELECT test1 FROM test1 WHERE test1 >= ? order by test1"))
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
    public void testExecuteUpdate() throws Exception
    {

    }
}