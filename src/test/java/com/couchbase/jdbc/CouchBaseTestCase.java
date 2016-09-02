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
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Before;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Created by davec on 2015-09-08.
 */

@Ignore  // Do not run this class, since it has no test methods of its own. Run tests derived from it, instead.
public class CouchBaseTestCase extends TestCase
{
    
	public Connection con;
	public static Properties properties;

    @BeforeClass
    public static void initialize() throws Exception
    {
		CouchBaseTestCase.properties = new Properties();
        properties.put(ConnectionParameters.SCAN_CONSISTENCY,"request_plus");
        properties.put(ConnectionParameters.USER,TestUtil.getUser());
        properties.put(ConnectionParameters.PASSWORD,TestUtil.getPassword());
        TestUtil.resetEnvironmentProperties(null);
    }
    
    @Before
    public void openConnection() throws Exception
    {
        con = DriverManager.getConnection(TestUtil.getURL(), CouchBaseTestCase.properties);
        assertNotNull(con);
    }
}
