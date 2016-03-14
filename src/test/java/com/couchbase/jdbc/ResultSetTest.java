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

import com.couchbase.jdbc.CBResultSet;

import org.junit.Rule;
import org.junit.Test;
import org.junit.After;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;

/**
 * Created by davec on 2015-07-15.
 */
@RunWith(JUnit4.class)
public class ResultSetTest extends CouchBaseTestCase
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

	@After
	public void cleanupBucket() throws Exception
	{
		JDBCTestUtils.deleteDataFromBucket("default");
	}
	
    @Test
    public void testAbsoluteNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.absolute(0);
            }
        }
    }

    @Test
    public void testBeforeFirstNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.beforeFirst();
            }
        }
    }
    @Test
    public void testIsAfterLastNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.isAfterLast();
            }
        }
    }
    @Test
    public void testIsBeforeFirstNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.isBeforeFirst();
            }
        }
    }

    @Test
    public void testRefreshRowNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.refreshRow();
            }
        }
    }
    @Test
    public void testRelativeNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.relative(1);
            }
        }
    }
    @Test
    public void testUpdateArrayNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateArray(1, null);
            }
        }
    }
    @Test
    public void testUpdateArray2NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateArray("foo", null);
            }
        }
    }
    @Test
    public void testUpdateAsciiStreamNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateAsciiStream(1, null);
            }
        }
    }
    @Test
    public void testUpdateAsciiStream1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateAsciiStream(1, null, 0);
            }
        }
    }
    @Test
    public void testUpdateAsciiStream2NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateAsciiStream(1, null, (long) 0);
            }
        }
    }
    @Test
    public void testUpdateAsciiStream3NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateAsciiStream("foo", null);
            }
        }
    }
    @Test
    public void testUpdateAsciiStream4NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateAsciiStream("foo", null, 0);
            }
        }
    }
    @Test
    public void testUpdateAsciiStream5NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateAsciiStream("foo", null, (long) 0);
            }
        }
    }
    @Test
    public void testUpdateBigDecimalNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBigDecimal(1, null);
            }
        }
    }
    @Test
    public void testUpdateBigDecimal1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBigDecimal("foo", null);
            }
        }
    }
    @Test
    public void testUpdateBinaryStreamNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBinaryStream(1, null);
            }
        }
    }
    @Test
    public void testUpdateBinaryStream1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBinaryStream(1, null);
            }
        }
    }
    @Test
    public void testUpdateBinaryStream2NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBinaryStream(1, null);
            }
        }
    }
    @Test
    public void testUpdateBinaryStream3NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBinaryStream(1, null, 0);

            }
        }
    }
    @Test
    public void testUpdateBinaryStream4NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBinaryStream(1, null, (long)0);


            }
        }
    }
    @Test
    public void testUpdateBinaryStream5NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBinaryStream("foo",null);

            }
        }
    }
    @Test
    public void testUpdateBinaryStream6NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBinaryStream("foo",null, 0);

            }
        }
    }
    @Test
    public void testUpdateBinaryStream7NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBinaryStream("foo", null, 0);
            }
        }
    }
    @Test
    public void testUpdateBlobNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBlob(1, (Blob) null);
            }
        }
    }
    @Test
    public void testUpdateBlob1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBlob(1, (InputStream)null);
            }
        }
    }
    @Test
    public void testUpdateBlob2NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBlob(1, null, 0);
            }
        }
    }
    @Test
    public void testUpdateBlob3NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBlob("foo", (Blob)null);

            }
        }
    }
    @Test
    public void testUpdateBlob4NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBlob("foo", (InputStream)null);
            }
        }
    }
    @Test
    public void testUpdateBlob5NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBlob("foo", null, 0);
            }
        }

    }
    @Test
    public void testUpdateBooleanNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBoolean(1,true);

            }
        }

    }
    @Test
    public void testUpdateBoolean1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBoolean("foo", true);
            }
        }

    }
    @Test
    public void testUpdateByteNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateByte(1, (byte)1);

            }
        }

    }
    @Test
    public void testUpdateByte1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateByte("foo", (byte) 1);
            }
        }

    }

    @Test
    public void testUpdateBytesNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBytes(1, "s".getBytes());

            }
        }

    }
    @Test
    public void testUpdateBytes1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateBytes("foo","s".getBytes());
            }
        }

    }

    @Test
    public void testUpdateCharacterStreamNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateCharacterStream(1, null);
            }
        }

    }

    @Test
    public void testUpdateCharacterStream1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateCharacterStream("foo", null, (long)0);
            }
        }

    }
    @Test
    public void testUpdateCharacterStream2NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateCharacterStream(1, null, 0);
            }
        }

    }
    @Test
    public void testUpdateCharacterStream3NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateCharacterStream(1, null, (long)0);
            }
        }

    }
    @Test
    public void testUpdateCharacterStream4NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateCharacterStream("foo", null);
            }
        }

    }
    @Test
    public void testUpdateCharacterStream5NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateCharacterStream("foo", null, 0);
            }
        }

    }


    @Test
    public void testUpdateCLobNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateClob(1, (Clob) null);
            }
        }

    }
    @Test
    public void testUpdateCLob1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateClob(1, (Clob) null);
            }
        }

    }
    @Test
    public void testUpdateCLob2NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateClob(1, null, 0);

            }
        }

    }
    @Test
    public void testUpdateCLob3NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateClob("foo", (Clob)null);
            }
        }

    }
    @Test
    public void testUpdateCLob4NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateClob("foo", null, 0);
            }
        }

    }
    @Test
    public void testUpdateNCharacterStreamNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateNCharacterStream(1, null);
            }
        }

    }

    @Test
    public void testUpdateNCharacterStream1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateNCharacterStream(1, null);
            }
        }

    }
    @Test
    public void testUpdateNCharacterStream2NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateNCharacterStream(1, null, 0);
            }
        }

    }
    @Test
    public void testUpdateNCharacterStream3NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateNCharacterStream("foo", null);
            }
        }

    }
    @Test
    public void testUpdateNCharacterStream4NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateNCharacterStream("foo", null, 0);
            }
        }

    }
    @Test
    public void testUpdateNCharacterStream5NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateNCharacterStream("foo", null, 0);
            }
        }

    }

    @Test
    public void testUpdateNCLobNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateNClob(1, (NClob) null);
            }
        }

    }

    @Test
    public void testUpdateNCLob1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateNClob(1, (Reader) null);
            }
        }

    }
    @Test
    public void testUpdateNCLob2NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateNClob(1, null, 0);
            }
        }

    }
    @Test
    public void testUpdateNCLob3NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateNClob("foo", (NClob) null);
            }
        }

    }
    @Test
    public void testUpdateNCLob4NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateNClob("foo", (Reader) null);
            }
        }

    }
    @Test
    public void testUpdateNCLob5NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateNClob("foo", null, 0);
            }
        }

    }


    @Test
    public void testUpdateNStringNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateNString(1, null);
            }
        }

    }

    @Test
    public void testUpdateNString1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateNString("foo", null);            }
        }

    }

    @Test
    public void testUpdateNullNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateNull(1);
            }
        }

    }

    @Test
    public void testUpdateNull1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateNull("foo");            }
        }

    }

    @Test
    public void testUpdateDateNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateDate(1, null);
            }
        }

    }

    @Test
    public void testUpdateDate1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateDate("foo", null);
            }

        }
    }

    @Test
    public void testUpdateDoubleNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateDouble(1, 0);
            }
        }

    }

    @Test
    public void testUpdateDouble1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateDouble("foo", 0);
            }

        }
    }


    @Test
    public void testUpdateFloatNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateFloat(1, 0);
            }
        }

    }

    @Test
    public void testUpdateFloat1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateFloat("foo", 0);
            }

        }
    }


    @Test
    public void testUpdateIntNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateInt(1, 0);
            }
        }

    }

    @Test
    public void testUpdateInt1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateInt("foo", 0);
            }

        }
    }
    @Test
    public void testUpdateLongNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateLong(1, 0);
            }
        }

    }

    @Test
    public void testUpdateLong1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateLong("foo", 0);
            }

        }
    }

    @Test
    public void testUpdateObjectNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateObject(1, null);
            }
        }

    }

    @Test
    public void testUpdateObject1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateObject("foo", null);
            }

        }
    }

    @Test
    public void testUpdateObject2NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateObject(1, null, 0);
            }
        }

    }

    @Test
    public void testUpdateObject3NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateObject("foo", null, 0);
            }

        }
    }

    @Test
    public void testUpdateObject4NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateObject("foo", null, Types.ARRAY);
            }

        }
    }

    @Test
    public void testUpdateRefNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateRef(1, null);
            }

        }
    }

    @Test
    public void testUpdateRef1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateRef("foo", null);
            }

        }
    }
    @Test
    public void testUpdateRowNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateRow();
            }

        }
    }

    @Test
    public void testUpdateRowIdNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateRowId(1,null);
            }

        }
    }
    @Test
    public void testUpdateRowId1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateRowId("foo",null);
            }

        }
    }
    @Test
    public void testUpdateTimeNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateTime(1, null);
            }

        }
    }
    @Test
    public void testUpdateTime1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateTime("foo", null);
            }

        }
    }
    @Test
    public void testUpdateTimestampNotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateTimestamp(1, null);
            }

        }
    }
    @Test
    public void testUpdateTimestamp1NotImplemented() throws Exception
    {
        try (Statement statement = con.createStatement())
        {
            assertNotNull(statement);
            try (ResultSet rs = statement.executeQuery("select 1 as foo"))
            {
                assertTrue(rs.next());
                expectedException.expect(SQLFeatureNotSupportedException.class);
                rs.updateTimestamp("foo",null);
            }

        }
    }

    @Test
    public void testGetString() throws Exception
    {
        try (Statement stmt = con.createStatement())
         {
             try (ResultSet rs = stmt.executeQuery("select now_str() as cur_time, 1 as one, true as bool" ))
             {
                 assertTrue(rs.next());
                 String string = rs.getString("cur_time");
                 assertNotNull(string);

                 string = rs.getString("one");
                 assertEquals("1",string);

                 string = rs.getString("bool");
                 assertEquals("true", string);



             }
         }
    }

    @Test
    public void testGetDate() throws Exception
    {
        try (Statement stmt = con.createStatement())
        {
            try (ResultSet rs = stmt.executeQuery("select now_str() as cur_time"))
            {
                assertTrue(rs.next());
                Date date = rs.getDate("cur_time");
                assertNotNull(date);

                date = rs.getDate(1);
                assertNotNull(date);

                String s = (String)rs.getObject("cur_time");
                assertNotNull(s);

                s = (String)rs.getObject(1);
                assertNotNull(s);

                date=rs.getObject(1, Date.class);
                assertNotNull(date);


            }
        }
    }

    @Test
    public void testIsNull() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1, "byte1");
            preparedStatement.setNull(2, Types.INTEGER);

            assertEquals(1, preparedStatement.executeUpdate());
            try (Statement stmt = con.createStatement())
            {
                try (CBResultSet rs = (CBResultSet) stmt.executeQuery("select name, phone, state, default from default where meta(default).id='byte1'"))
                {
                    assertTrue(rs.next());
                    assertEquals(0,rs.getByte("default"));
                    assertTrue(rs.wasNull());

                    assertEquals(0,rs.getShort("default"));
                    assertTrue(rs.wasNull());

                    assertEquals(0,rs.getInt("default"));
                    assertTrue(rs.wasNull());

                    assertEquals(0,rs.getLong("default"));
                    assertTrue(rs.wasNull());

                    assertEquals(0,rs.getDouble("default"),0);
                    assertTrue(rs.wasNull());

                    assertNull(rs.getBigDecimal("default"));
                    assertTrue(rs.wasNull());

                    assertNull(rs.getString("default"));
                    assertTrue(rs.wasNull());

                    assertNull(rs.getObject("default"));
                    assertTrue(rs.wasNull());

                    assertNull(rs.getSQLJSON("default"));
                    assertTrue(rs.wasNull());

                    assertNull(rs.getArray("default"));
                    assertTrue(rs.wasNull());

                    assertNull(rs.getURL("default"));
                    assertTrue(rs.wasNull());

                    assertNull(rs.getAsciiStream("default"));
                    assertTrue(rs.wasNull());

                    assertNull(rs.getBinaryStream("default"));
                    assertTrue(rs.wasNull());

                    assertNull(rs.getUnicodeStream("default"));
                    assertTrue(rs.wasNull());


                }
            }
        }
    }

    @Test
    public void testWasMissing() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1, "byte1");
            preparedStatement.setByte(2, (byte) 1);

            assertEquals(1, preparedStatement.executeUpdate());
            try (Statement stmt = con.createStatement())
            {
                try (CBResultSet rs = (CBResultSet) stmt.executeQuery("select name, phone, state, default from default where meta(default).id='byte1'"))
                {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getByte("name"));
                    assertTrue(rs.wasMissing());
                    assertEquals(1,rs.getByte("default"));
                    assertFalse(rs.wasMissing());

                    assertEquals(0, rs.getShort("name"));
                    assertTrue(rs.wasMissing());
                    assertEquals(1,rs.getShort("default"));
                    assertFalse(rs.wasMissing());

                    assertEquals(0, rs.getInt("name"));
                    assertTrue(rs.wasMissing());
                    assertEquals(1,rs.getInt("default"));
                    assertFalse(rs.wasMissing());

                    assertEquals(0, rs.getLong("name"));
                    assertTrue(rs.wasMissing());
                    assertEquals(1,rs.getLong("default"));
                    assertFalse(rs.wasMissing());

                    assertEquals(0, rs.getDouble("name"),0);
                    assertTrue(rs.wasMissing());
                    assertEquals(1,rs.getDouble("default"),0);
                    assertFalse(rs.wasMissing());

                    assertEquals(null, rs.getBigDecimal("name"));
                    assertTrue(rs.wasMissing());
                    assertEquals(BigDecimal.ONE,rs.getBigDecimal("default"));
                    assertFalse(rs.wasMissing());

                    assertNull(rs.getString("name"));
                    assertTrue(rs.wasMissing());
                    assertEquals("1",rs.getString("default"));
                    assertFalse(rs.wasMissing());

                    assertNull( rs.getObject("name") );
                    assertTrue(rs.wasMissing());
                    assertEquals(1,rs.getObject("default"));
                    assertFalse(rs.wasMissing());

                    assertNull( rs.getSQLJSON("name") );
                    assertTrue(rs.wasMissing());
                    assertEquals(1,rs.getSQLJSON("default").getInt());
                    assertFalse(rs.wasMissing());

                    assertNull( rs.getAsciiStream("name") );
                    assertTrue(rs.wasMissing());

                    assertNull(rs.getBinaryStream("name"));
                    assertTrue(rs.wasMissing());

                    assertNull(rs.getUnicodeStream("name"));
                    assertTrue(rs.wasMissing());



                }
            }
        }
    }

    @Test
    public void testGetTimestamp() throws Exception
    {
        try (Statement stmt = con.createStatement())
        {
            try (ResultSet rs = stmt.executeQuery("select '2015-09-14 09:54:00.12345' as cur_time"))
            {
                assertTrue(rs.next());
                Timestamp ts = rs.getTimestamp("cur_time");
                assertNotNull(ts);

                Calendar calendar1 = Calendar.getInstance();
                calendar1.setTime(ts);

                // month is 0 based
                assertEquals(8, calendar1.get(Calendar.MONTH));
                assertEquals(14, calendar1.get(Calendar.DAY_OF_MONTH));
                assertEquals(2015, calendar1.get(Calendar.YEAR));

                assertEquals(9, calendar1.get(Calendar.HOUR_OF_DAY));
                assertEquals(54, calendar1.get(Calendar.MINUTE));
                assertEquals(0, calendar1.get(Calendar.SECOND));

                assertEquals(0, calendar1.get(Calendar.SECOND));

                assertEquals(123450000, ts.getNanos());

                ts=rs.getObject("cur_time", Timestamp.class);
                assertNotNull(ts);
                ts=rs.getObject(1, Timestamp.class);
                assertNotNull(ts);

                calendar1 = Calendar.getInstance();
                calendar1.setTime(ts);

                // month is 0 based
                assertEquals(8, calendar1.get(Calendar.MONTH));
                assertEquals(14, calendar1.get(Calendar.DAY_OF_MONTH));
                assertEquals(2015, calendar1.get(Calendar.YEAR));

                assertEquals(9, calendar1.get(Calendar.HOUR_OF_DAY));
                assertEquals(54, calendar1.get(Calendar.MINUTE));
                assertEquals(0, calendar1.get(Calendar.SECOND));

                assertEquals(0, calendar1.get(Calendar.SECOND));

                assertEquals(123450000, ts.getNanos());

            }
        }
    }

    Class <?> args[] = {java.lang.String.class};
    @Test
    public void testBadColumnName() throws Exception
    {
        try(PreparedStatement preparedStatement = con.prepareStatement("insert into default(key,value) values (?,?)"))
        {
            preparedStatement.setString(1, "byte1");
            preparedStatement.setByte(2, (byte) 1);

            assertEquals(1, preparedStatement.executeUpdate());

            try(Statement statement = con.createStatement())
            {
                try (ResultSet rs = statement.executeQuery("select * from default where meta(default).id='byte1'"))
                {
                    assertTrue(rs.next());

                    for(String methodName:TestUtil.getSuppportedResultSetGetters())
                    {
                        try
                        {
                            Method m = rs.getClass().getMethod(methodName, args);
                            m.invoke(rs, "foo");

                            assertTrue("Foo column does not exist", false);
                        }
                        catch (InvocationTargetException ex)
                        {
                            Throwable target = ex.getTargetException();
                            if (target.getMessage() == null )
                            {
                                fail(target.getMessage());
                            }
                            if ( target != null && target.getMessage().startsWith("ResultSet does not contain"))
                            {
                                assertTrue("Should throw Result does not contain ", true);
                            }
                            else
                            {
                                fail(ex.getMessage());
                            }
                        }

                    }


                }
            }
        }
    }
}
