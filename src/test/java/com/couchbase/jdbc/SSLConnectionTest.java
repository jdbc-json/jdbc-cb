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

import com.couchbase.jdbc.ConnectionParameters;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Created by davec on 2015-09-10.
 */
@RunWith(JUnit4.class)
public class SSLConnectionTest extends TestCase
{
    @Test
    public void openConnection() throws Exception
    {
        Properties properties = new Properties();

        properties.put(ConnectionParameters.SCAN_CONSISTENCY,"request_plus");
        properties.put(ConnectionParameters.USER, TestUtil.getUser());
        properties.put(ConnectionParameters.PASSWORD,TestUtil.getPassword());
        properties.put(ConnectionParameters.ENABLE_SSL,"true");
        Connection con = DriverManager.getConnection(TestUtil.getSSLUrl(), properties);

        assertNotNull(con);
        try(Statement statement = con.createStatement())
        {
            try(ResultSet rs = statement.executeQuery("select 1"))
            {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }

        }


    }
}
