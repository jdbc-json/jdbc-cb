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
    @Test
    public void createStatement() throws Exception
    {
        Connection con = DriverManager.getConnection(TestUtil.getURL(), TestUtil.getUser(), TestUtil.getPassword());
        assertNotNull(con);
        Statement statement = con.createStatement();
        assertNotNull(statement);

        con.close();
    }
    @Test
    public void emptyResult() throws Exception
    {
        Connection con = DriverManager.getConnection(TestUtil.getURL(), TestUtil.getUser(), TestUtil.getPassword());
        assertNotNull(con);
        Statement statement = con.createStatement();
        assertNotNull(statement);

        ResultSet rs = statement.executeQuery("select * from test1");
        assertFalse(rs.next());

    }
    @Test
    public void simpleSelect() throws Exception
    {
        Connection con = DriverManager.getConnection(TestUtil.getURL(), TestUtil.getUser(), TestUtil.getPassword());
        assertNotNull(con);
        Statement statement = con.createStatement();
        assertNotNull(statement);

        ResultSet rs = statement.executeQuery("select 1");
        while (rs.next())
        {
            assertEquals("1",rs.getString(1));
        }
    }
}
