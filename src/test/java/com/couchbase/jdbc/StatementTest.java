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

import org.junit.Test;
import org.junit.After;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by davec on 2015-02-26.
 */
@RunWith(JUnit4.class)
public class StatementTest extends CouchBaseTestCase
{
	@After
	public void cleanupBucket() throws Exception
	{
		JDBCTestUtils.deleteDataFromBucket("default");
	}
	
    @Test
    public void createStatement() throws Exception
    {
        Statement statement = con.createStatement();
        assertNotNull(statement);

    }
    @Test
    public void emptyResult() throws Exception
    {
        Statement statement = con.createStatement();
        assertNotNull(statement);

        ResultSet rs = statement.executeQuery("select * from default");
        assertFalse(rs.next());

    }
    @Test
    public void simpleSelect() throws Exception
    {
        Statement statement = con.createStatement();
        assertNotNull(statement);

        ResultSet rs = statement.executeQuery("select 1");

        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));

    }

    @Test
    public void simpleInsert() throws Exception
    {
        assertNotNull(con);
        Statement statement = con.createStatement();
        assertNotNull(statement);

        for (int i = 0; i++< 100;)
        {

            int inserted = statement.executeUpdate("INSERT INTO default  (KEY, VALUE) VALUES ( 'K" + i + "', { 'id': " + i + ", 'val': " + i + " })");
            assertEquals(1, inserted);
        }
        Thread.sleep(5000);

        ResultSet resultSet = statement.executeQuery("select count(1) as test_count from default");
        assertTrue(resultSet.next());
        assertEquals(100,resultSet.getInt("test_count"));

        resultSet = statement.executeQuery("select val from default order by val");
        for (int i=0; resultSet.next(); i++)
        {
            assertEquals(i+1, resultSet.getInt(1));
        }

        resultSet = statement.executeQuery("select raw val from default order by val");
        for (int i=0; resultSet.next(); i++)
        {
            assertTrue(resultSet.getInt(1)>0);
        }

// A known problem, assigned issue #16.
        boolean hasResultSet = statement.execute("update default set val=0 returning val");
        if ( hasResultSet )
        {
            resultSet = statement.getResultSet();
            for (int i=0; resultSet.next(); i++)
            {
                assertEquals(0, resultSet.getInt(1));
            }

        }

        statement.executeUpdate("delete from default");
        Thread.sleep(5000);

        resultSet = statement.executeQuery("select count(1) as count from default");
        assertTrue(resultSet.next());
        assertEquals(0, resultSet.getInt(1));

    }
    @Test
    public void getAllTypes() throws Exception
    {
        Integer []foo = new Integer[]{1, 2, 3, 5, 8};
        Object array = Arrays.asList(foo);


        Map object = new HashMap();
        object.put("a1","Object");

        Statement statement = con.createStatement();
        assertNotNull(statement);

        ResultSet resultSet = statement.executeQuery("SELECT true as c1, 1 as c2, 3.14 as c3,  'Hello World!' as c4, [1,2,3,5,8] as c5, { 'a1': 'Object' } as c6");

        assertTrue(resultSet.next());

        assertTrue(resultSet.getBoolean(1));
        assertTrue(resultSet.getBoolean("c1"));

        assertEquals(1,resultSet.getInt(2));
        assertEquals(1,resultSet.getInt("c2"));


        assertEquals(3.14F, resultSet.getFloat(3), 0.0f);
        assertEquals(3.14F, resultSet.getFloat("c3"), 0.0f);

        assertEquals("Hello World!",resultSet.getString(4));
        assertEquals("Hello World!",resultSet.getString("c4"));

        assertTrue(Arrays.equals(foo, (Object[]) resultSet.getArray(5).getArray()));
        assertTrue(Arrays.equals(foo, (Object[]) resultSet.getArray("c5").getArray()));

        Object foo2 = resultSet.getObject(6);
        assertEquals(object, resultSet.getObject(6));
        assertEquals(object, resultSet.getObject("c6"));


    }


}
