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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by davec on 2015-07-21.
 */
@RunWith(JUnit4.class)
public class ConnectionTest extends CouchBaseTestCase
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();


    @Test
    public void testCreateArray() throws Exception
    {
        Object [] attributes = {"1", "2"};

        Array array = con.createArrayOf("int", attributes);
        assertNotNull(array);
        con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.createArrayOf("int",attributes);
    }

    @Test
    public void testCreateStructNotImplemented() throws Exception
    {
        Object [] attributes = {"1", "2"};

        expectedException.expect(SQLFeatureNotSupportedException.class);
        CouchBaseTestCase.con.createStruct("sometype", attributes);

    }
    @Test
    public void testGetUserName() throws Exception
    {
        assertSame(TestUtil.getUser(), ((CBConnection)con).getUserName());
    }

    @Test
    public void testGetPassword() throws Exception
    {
        assertSame(TestUtil.getPassword(), ((CBConnection)con).getPassword());
    }

    @Test
    public void testCreateStatement() throws Exception
    {
        assertNotNull(con.createStatement());
        con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.createStatement();
    }

    @Test
    public void testPrepareStatement() throws Exception
    {
        assertNotNull(con.prepareStatement("select 1"));
        con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.prepareStatement("");
    }

    @Test
    public void testPrepareCall() throws Exception
    {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        CouchBaseTestCase.con.prepareCall("SELECT foo");
    }

    @Test
    public void testNativeSQL() throws Exception
    {
        assertSame(con.nativeSQL("select * from foo"), "select * from foo");
        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.nativeSQL("");
    }

    @Test
    public void testSetAutoCommit() throws Exception
    {
        // this is a no-op just make sure it doesn't fail
        con.setAutoCommit(true);
        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.setAutoCommit(false);

    }

    @Test
    public void testGetAutoCommit() throws Exception
    {
        assertFalse(con.getAutoCommit());
        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.getAutoCommit();
    }

    @Test
    public void testCommit() throws Exception
    {
    	CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.commit();
    }

    @Test
    public void testRollback() throws Exception
    {
    	CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.rollback();
    }

    @Test
    public void testClose() throws Exception
    {
        // should do nothing for both of these
    	CouchBaseTestCase.con.close();
    	CouchBaseTestCase.con.close();
    }

    @Test
    public void testIsClosed() throws Exception
    {
    	CouchBaseTestCase.con.close();
        assertTrue(con.isClosed());
    }

    @Test
    public void testGetMetaData() throws Exception
    {
        assertNotNull(con.getMetaData());
        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.getMetaData();
    }

    @Test
    public void testSetReadOnly() throws Exception
    {
        con.setReadOnly(true);

        Statement statement = CouchBaseTestCase.con.createStatement();
        try
        {
            statement.executeUpdate("INSERT INTO default  (KEY, VALUE) VALUES ( 'K1', 1)");
        }
        catch (SQLException ex)
        {
            assertTrue(ex.getCause().getMessage().startsWith("The server or request is read-only"));
        }
        finally
        {
        	CouchBaseTestCase.con.setReadOnly(true);
        }

        CouchBaseTestCase.con.close();
        try
        {
        	CouchBaseTestCase.con.setReadOnly(true);
        }
        catch (SQLException ex)
        {
            assertTrue(ex.getMessage().startsWith("Connection is closed"));
        }

    }

    @Test
    public void testIsReadOnly() throws Exception
    {
    	CouchBaseTestCase.con.setReadOnly(false);
        assertFalse(con.isReadOnly());
        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        con.isReadOnly();
    }

    @Test
    public void testSetCatalog() throws Exception
    {
    	CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Connection is closed");
        CouchBaseTestCase.con.setCatalog("system");

    }

    @Test
    public void testGetCatalog() throws Exception
    {
    	CouchBaseTestCase.con.setCatalog("system");
        assertEquals(con.getCatalog(),"system");

        CouchBaseTestCase.con.close();
        expectedException.expectMessage("Connection is closed");
        expectedException.expect(SQLException.class);

        con.getCatalog();
    }

    @Test
    public void testSetTransactionIsolation() throws Exception
    {
    	CouchBaseTestCase.con.setTransactionIsolation(Connection.TRANSACTION_NONE);
    	CouchBaseTestCase. con.close();
        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Connection is closed");
        con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    }

    @Test
    public void testGetTransactionIsolation() throws Exception
    {
    	CouchBaseTestCase.con.setTransactionIsolation(Connection.TRANSACTION_NONE);
        assertEquals(con.getTransactionIsolation(), Connection.TRANSACTION_NONE);

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Connection is closed");

        con.getTransactionIsolation();
    }

    @Test
    public void testGetWarnings() throws Exception
    {
    	CouchBaseTestCase.con.getWarnings();
    	CouchBaseTestCase.con.close();

        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Connection is closed");
        CouchBaseTestCase.con.getWarnings();
    }

    @Test
    public void testClearWarnings() throws Exception
    {
    	CouchBaseTestCase.con.clearWarnings();
    	CouchBaseTestCase.con.close();

        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Connection is closed");
        CouchBaseTestCase.con.clearWarnings();

    }

    @Test
    public void testCreateStatement1() throws Exception
    {
        assertNotNull(con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));

        expectedException.expect(SQLFeatureNotSupportedException.class);
        CouchBaseTestCase.con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);

        expectedException.expect(SQLFeatureNotSupportedException.class);
        CouchBaseTestCase.con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);

        expectedException.expect(SQLFeatureNotSupportedException.class);
        CouchBaseTestCase.con.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

    }

    @Test
    public void testPrepareStatement1() throws Exception
    {
        String sql = "select 1 as one";
        assertNotNull(con.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));

        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.prepareStatement");
        CouchBaseTestCase.con.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.prepareStatement");
        CouchBaseTestCase.con.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);

        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.prepareStatement");
        CouchBaseTestCase.con.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

    }

    @Test
    public void testPrepareCall1() throws Exception
    {
        String sql = "select 1 as one";

        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.prepareCall");
        CouchBaseTestCase.con.prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

    }

    @Test
    public void testGetTypeMap() throws Exception
    {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.getTypeMap");
        CouchBaseTestCase.con.getTypeMap();

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.getTypeMap();
    }


    @Test
    public void testSetTypeMap() throws Exception
    {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.setTypeMap");
        CouchBaseTestCase.con.setTypeMap(new HashMap<String, Class<?>>());

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.setTypeMap(new HashMap<String, Class<?>>());

    }

    @Test
    public void testSetHoldability() throws Exception
    {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.setHoldability");
        CouchBaseTestCase.con.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);

    }

    @Test
    public void testGetHoldability() throws Exception
    {
        assertEquals(con.getHoldability(), ResultSet.CLOSE_CURSORS_AT_COMMIT);

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);

    }

    @Test
    public void testSetSavepoint() throws Exception
    {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.setSavepoint");
        CouchBaseTestCase.con.setSavepoint();

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.setSavepoint();
    }

    @Test
    public void testSetSavepoint1() throws Exception
    {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.setSavepoint");
        CouchBaseTestCase.con.setSavepoint("foo");

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.setSavepoint("foo");
    }

    @Test
    public void testRollback1() throws Exception
    {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.rollback");
        CouchBaseTestCase.con.rollback(null);

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.rollback(null);

    }

    @Test
    public void testReleaseSavepoint() throws Exception
    {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.releaseSavepoint");
        CouchBaseTestCase.con.releaseSavepoint(null);

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.releaseSavepoint(null);

    }

    @Test
    public void testCreateStatement2() throws Exception
    {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.createStatement");
        CouchBaseTestCase.con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);

    }

    @Test
    public void testPrepareStatement2() throws Exception
    {
        String sql="select 1";
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.prepareStatement");
        CouchBaseTestCase.con.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Test
    public void testPrepareCall2() throws Exception
    {
        String sql="select 1";
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.prepareCall");
        CouchBaseTestCase.con.prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);

    }

    @Test
    public void testPrepareStatement3() throws Exception
    {
        String sql="select 1";
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.prepareStatement");
        CouchBaseTestCase.con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

    }

    @Test
    public void testPrepareStatement4() throws Exception
    {
        String sql="select 1";
        int columns[] = {1,2};

        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.prepareStatement");
        CouchBaseTestCase.con.prepareStatement(sql, columns);

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.prepareStatement(sql,columns );

    }

    @Test
    public void testPrepareStatement5() throws Exception
    {
        String sql="insert into default (key,value) values(?,?)";
        String columns[] = {"default"};

        assertNotNull(con.prepareStatement(sql, columns));

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.prepareStatement(sql,columns );

    }

    @Test
    public void testCreateClob() throws Exception
    {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.createClob");
        CouchBaseTestCase.con.createClob();
    }

    @Test
    public void testCreateBlob() throws Exception
    {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.createBlob");
        CouchBaseTestCase.con.createBlob();

    }

    @Test
    public void testCreateNClob() throws Exception
    {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.createNClob");
        CouchBaseTestCase.con.createNClob();

    }

    @Test
    public void testCreateSQLXML() throws Exception
    {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.createSQLXML");
        CouchBaseTestCase.con.createSQLXML();

    }

    @Test
    public void testIsValid() throws Exception
    {
        assertTrue(con.isValid(0));
        CouchBaseTestCase.con.close();
        assertFalse(con.isValid(0));
    }

    @Test
    public void testSetClientInfo() throws Exception
    {

    }

    @Test
    public void testSetClientInfo1() throws Exception
    {

    }

    @Test
    public void testGetClientInfo() throws Exception
    {

    }

    @Test
    public void testGetClientInfo1() throws Exception
    {

    }


    @Test
    public void testCreateStruct() throws Exception
    {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("com.couchbase.CBConnection.createStruct");
        CouchBaseTestCase.con.createStruct(null, null);

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.createStruct(null, null);

    }

    @Test
    public void testSetSchema() throws Exception
    {
        con.setSchema("SYSTEM");

        try (Statement statement=con.createStatement())
        {
            try (ResultSet rs = statement.executeQuery("select * from keyspaces where name ='default'"))
            {
                assertTrue(rs.next());
                Map map = (Map)rs.getObject("keyspaces");
                assertEquals(map.get("name"),"default");
            }
        }

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.setSchema("SYSTEM");
    }

    @Test
    public void testGetSchema() throws Exception
    {
    	CouchBaseTestCase.con.setSchema("SYSTEM");

        assertEquals("SYSTEM",con.getSchema());

        CouchBaseTestCase.con.close();
        expectedException.expect(SQLException.class);
        CouchBaseTestCase.con.getSchema();

    }

    @Test
    public void testAbort() throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool(5);
        CouchBaseTestCase.con.abort(executor);

        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Executor is null");
        CouchBaseTestCase.con.abort(null);
    }

    @Test
    public void testSetNetworkTimeout() throws Exception
    {

    }

    @Test
    public void testGetNetworkTimeout() throws Exception
    {

    }

    @Test
    public void testCheckClosed() throws Exception
    {

    }

    @Test
    public void testCreateSQLJSON() throws Exception
    {

    }

}
