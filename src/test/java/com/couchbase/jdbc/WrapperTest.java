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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.couchbase.jdbc.CBConnection;
import com.couchbase.jdbc.CBStatement;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by davec on 2015-09-16.
 */
@RunWith(JUnit4.class)
public class WrapperTest extends CouchBaseTestCase
{
    Statement statement;
    /**
     * This interface is private, and so cannot be supported by any wrapper
     *
     */
    private interface PrivateInterface {
    }


    @Before
    public void setup() throws Exception
    {
        statement=con.createStatement();
    }
        

    @Test
    public void testConnectionIsWrapperForPrivate() throws SQLException {
        assertFalse(con.isWrapperFor(PrivateInterface.class));
    }

    @Test
    public void testConnectionIsWrapperForConnection() throws SQLException {
        assertTrue(con.isWrapperFor(Connection.class));
    }

    @Test
    public void testConnectionIsWrapperForCBConnection() throws SQLException {
        assertTrue(con.isWrapperFor(CBConnection.class));
    }

    @Test
    public void testConnectionUnwrapPrivate() throws SQLException {
        try {
            con.unwrap(PrivateInterface.class);
            fail("unwrap of non-wrapped interface should fail");
        }
        catch (SQLException e) {
            // Ok
        }
    }

    @Test
    public void testConnectionUnwrapConnection() throws SQLException {
        Object v = con.unwrap(Connection.class);
        assertNotNull(v);
        assertTrue(v instanceof Connection);
    }

    @Test
    public void testConnectionUnwrapCBConnection() throws SQLException {
        Object v = con.unwrap(CBConnection.class);
        assertNotNull(v);
        assertTrue(v instanceof CBConnection);
    }

    @Test
    public void testStatementIsWrapperForPrivate() throws SQLException {
        assertFalse(statement.isWrapperFor(PrivateInterface.class));
    }

    @Test
    public void testStatementIsWrapperForStatement() throws SQLException {
        assertTrue(statement.isWrapperFor(Statement.class));
    }

    @Test
    public void testStatementIsWrapperForCBStatement() throws SQLException {
        assertTrue(statement.isWrapperFor(CBStatement.class));
    }

    @Test
    public void testStatementUnwrapPrivate() throws SQLException {
        try {
            statement.unwrap(PrivateInterface.class);
            fail("unwrap of non-wrapped interface should fail");
        }
        catch (SQLException e) {
            // Ok
        }
    }

    @Test
    public void testStatementUnwrapStatement() throws SQLException {
        Object v = statement.unwrap(Statement.class);
        assertNotNull(v);
        assertTrue(v instanceof Statement);
    }

    @Test
    public void testStatementUnwrapCBStatement() throws SQLException {
        Object v = statement.unwrap(CBStatement.class);
        assertNotNull(v);
        assertTrue(v instanceof CBStatement);
    }
}
