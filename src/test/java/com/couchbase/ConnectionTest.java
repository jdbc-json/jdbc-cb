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

import java.sql.*;

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
        con.close();
        expectedException.expect(SQLException.class);
        con.createArrayOf("int",attributes);
    }

    @Test
    public void testCreateStructNotImplemented() throws Exception
    {
        Object [] attributes = {"1", "2"};

        expectedException.expect(SQLFeatureNotSupportedException.class);
        con.createStruct("sometype", attributes);

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
        con.createStatement();
    }

    @Test
    public void testPrepareStatement() throws Exception
    {
        assertNotNull(con.prepareStatement("select 1"));
        con.close();
        expectedException.expect(SQLException.class);
        con.prepareStatement("");
    }

    @Test
    public void testPrepareCall() throws Exception
    {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        con.prepareCall("SELECT foo");
    }

    @Test
    public void testNativeSQL() throws Exception
    {
        assertSame(con.nativeSQL("select * from foo"), "select * from foo");
        con.close();
        expectedException.expect(SQLException.class);
        con.nativeSQL("");
    }

    @Test
    public void testSetAutoCommit() throws Exception
    {
        // this is a no-op just make sure it doesn't fail
        con.setAutoCommit(true);
        con.close();
        expectedException.expect(SQLException.class);
        con.setAutoCommit(false);

    }

    @Test
    public void testGetURL() throws Exception
    {
        assertSame(((CBConnection)con).getURL(), TestUtil.getURL());
        con.close();
        expectedException.expect(SQLException.class);
        ((CBConnection)con).getURL();
    }

    @Test
    public void testGetAutoCommit() throws Exception
    {
        assertFalse(con.getAutoCommit());
        con.close();
        expectedException.expect(SQLException.class);
        con.getAutoCommit();
    }

    @Test
    public void testCommit() throws Exception
    {
        con.close();
        expectedException.expect(SQLException.class);
        con.commit();
    }

    @Test
    public void testRollback() throws Exception
    {
        con.close();
        expectedException.expect(SQLException.class);
        con.rollback();
    }

    @Test
    public void testClose() throws Exception
    {
        // should do nothing for both of these
        con.close();
        con.close();
    }

    @Test
    public void testIsClosed() throws Exception
    {
        con.close();
        assertTrue(con.isClosed());
    }

    @Test
    public void testGetMetaData() throws Exception
    {
        assertNotNull(con.getMetaData());
        con.close();
        expectedException.expect(SQLException.class);
        con.getMetaData();
    }

    @Test
    public void testSetReadOnly() throws Exception
    {
        con.close();
        expectedException.expect(SQLException.class);
        con.setReadOnly(true);
    }

    @Test
    public void testIsReadOnly() throws Exception
    {
        con.setReadOnly(false);
        assertFalse(con.isReadOnly());
        con.close();
        expectedException.expect(SQLException.class);
        con.isReadOnly();
    }

    @Test
    public void testSetCatalog() throws Exception
    {
        con.close();
        expectedException.expect(SQLException.class);
        con.setCatalog("system");

    }

    @Test
    public void testGetCatalog() throws Exception
    {
        con.setCatalog("system");
        assertSame(con.getCatalog(),"system");

        con.close();
        expectedException.expect(SQLException.class);
        con.getCatalog();
    }

    @Test
    public void testSetTransactionIsolation() throws Exception
    {
        con.setTransactionIsolation(Connection.TRANSACTION_NONE);
        con.close();
        expectedException.expect(SQLException.class);
        con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    }

    @Test
    public void testGetTransactionIsolation() throws Exception
    {
        con.setTransactionIsolation(Connection.TRANSACTION_NONE);
        assertEquals(con.getTransactionIsolation(), Connection.TRANSACTION_NONE);

        con.close();
        expectedException.expect(SQLException.class);

        con.getTransactionIsolation();
    }

    @Test
    public void testGetWarnings() throws Exception
    {
        con.getWarnings();
        con.close();

        expectedException.expect(SQLException.class);
        con.getWarnings();
    }

    @Test
    public void testClearWarnings() throws Exception
    {
        con.clearWarnings();
        con.close();

        expectedException.expect(SQLException.class);
        con.clearWarnings();

    }

    @Test
    public void testCreateStatement1() throws Exception
    {
        assertNotNull(con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        expectedException.expect(SQLFeatureNotSupportedException.class);
        con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);

        expectedException.expect(SQLFeatureNotSupportedException.class);
        con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);

        expectedException.expect(SQLFeatureNotSupportedException.class);
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);

        con.close();
        expectedException.expect(SQLException.class);
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

    }

    @Test
    public void testPrepareStatement1() throws Exception
    {

    }

    @Test
    public void testPrepareCall1() throws Exception
    {

    }

    @Test
    public void testGetTypeMap() throws Exception
    {

    }

    @Test
    public void testSetTypeMap() throws Exception
    {

    }

    @Test
    public void testSetHoldability() throws Exception
    {

    }

    @Test
    public void testGetHoldability() throws Exception
    {

    }

    @Test
    public void testSetSavepoint() throws Exception
    {

    }

    @Test
    public void testSetSavepoint1() throws Exception
    {

    }

    @Test
    public void testRollback1() throws Exception
    {

    }

    @Test
    public void testReleaseSavepoint() throws Exception
    {

    }

    @Test
    public void testCreateStatement2() throws Exception
    {

    }

    @Test
    public void testPrepareStatement2() throws Exception
    {

    }

    @Test
    public void testPrepareCall2() throws Exception
    {

    }

    @Test
    public void testPrepareStatement3() throws Exception
    {

    }

    @Test
    public void testPrepareStatement4() throws Exception
    {

    }

    @Test
    public void testPrepareStatement5() throws Exception
    {

    }

    @Test
    public void testCreateClob() throws Exception
    {

    }

    @Test
    public void testCreateBlob() throws Exception
    {

    }

    @Test
    public void testCreateNClob() throws Exception
    {

    }

    @Test
    public void testCreateSQLXML() throws Exception
    {

    }

    @Test
    public void testIsValid() throws Exception
    {

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
    public void testCreateArrayOf() throws Exception
    {

    }

    @Test
    public void testCreateStruct() throws Exception
    {

    }

    @Test
    public void testSetSchema() throws Exception
    {

    }

    @Test
    public void testGetSchema() throws Exception
    {

    }

    @Test
    public void testAbort() throws Exception
    {

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
    public void testUnwrap() throws Exception
    {

    }

    @Test
    public void testIsWrapperFor() throws Exception
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
