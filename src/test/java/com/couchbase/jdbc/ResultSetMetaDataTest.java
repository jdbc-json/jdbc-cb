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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.Test;

import java.sql.*;

@RunWith(JUnit4.class)
public class ResultSetMetaDataTest extends CouchBaseTestCase
{
    static ResultSet resultSet;
    static ResultSetMetaData resultSetMetaData;
    
    @Before
    public void open() throws Exception
    {
        Statement statement = con.createStatement();
        assertNotNull(statement);

        resultSet = statement.executeQuery("SELECT true as c1, 1 as c2, 3.14 as c3,  'Hello World!' as c4, [1,2,3,5,8] as c5, { 'a1': 'Object' } as c6");
        assertTrue(resultSet.next());
        resultSetMetaData = resultSet.getMetaData();

    }

    @After
    public void closeConnection() throws Exception
    {
        assertNotNull(resultSet);
        resultSet.close();
        assertNotNull(con);
        con.close();
    }

    @Test
    public void testGetColumnCount() throws Exception
    {
        assertEquals(6,resultSetMetaData.getColumnCount());
    }

    @Test
    public void testIsAutoIncrement() throws Exception
    {
        for (int i=1;i<=6;i++)
        {
            assertFalse( resultSetMetaData.isAutoIncrement(i) );
        }
    }

    @Test
    public void testGetColumnLabel() throws Exception
    {
        for (int i=1;i<=6;i++)
        {
            assertEquals("c"+i,resultSetMetaData.getColumnLabel(i));
        }
    }

    @Test
    public void testGetColumnName() throws Exception
    {
        for (int i=1;i<=6;i++)
        {
            assertEquals("c"+i,resultSetMetaData.getColumnName(i));
        }

    }

    @Test
    public void testGetColumnType() throws Exception
    {

        int [] types = {Types.BOOLEAN, Types.NUMERIC,Types.NUMERIC, Types.VARCHAR, Types.ARRAY, Types.JAVA_OBJECT};
        for (int i=1;i<=6;i++)
        {
            assertEquals(types[i-1],resultSetMetaData.getColumnType(i));
        }

    }

    @Test
    public void testGetColumnTypeName() throws Exception
    {
        String [] types = {"boolean", "number", "number", "string", "array", "object" };
        for (int i=1;i<=6;i++)
        {
            assertEquals(types[i-1],resultSetMetaData.getColumnTypeName(i));
        }
    }
}