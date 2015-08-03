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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLFeatureNotSupportedException;

/**
 * Created by davec on 2015-07-21.
 */
@RunWith(JUnit4.class)
public class ConnectionTest extends TestCase
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

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
        con.createStatement().executeUpdate("delete from default");
        con.close();
    }

    @Test
    public void testCreateArray() throws Exception
    {
        Object [] attributes = {"1", "2"};

        Array array = con.createArrayOf("int", attributes);
        assertNotNull(array);
    }

    @Test
    public void testCreateStructNotImplemented() throws Exception
    {
        Object [] attributes = {"1", "2"};

        expectedException.expect(SQLFeatureNotSupportedException.class);
        con.createStruct("sometype", attributes);

    }

}
