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

import com.couchbase.jdbc.CBConnection;
import com.couchbase.jdbc.ConnectionParameters;
import com.couchbase.jdbc.connect.Cluster;
import com.couchbase.jdbc.core.ProtocolImpl;
import junit.framework.TestCase;

import org.boon.json.JsonFactory;
import org.boon.json.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

/**
 * Created by davec on 2015-09-02.
 */
@RunWith(JUnit4.class)
public class ClusterTest extends TestCase
{
    Connection con;
    @Before
    public void openConnection() throws Exception
    {
        Properties properties = new Properties();
        TestUtil.resetEnvironmentProperties(null);
        properties.put(ConnectionParameters.CONNECTION_TIMEOUT,"1000");
        properties.put(ConnectionParameters.USER,TestUtil.getUser());
        properties.put(ConnectionParameters.PASSWORD,TestUtil.getPassword());
        con = DriverManager.getConnection(TestUtil.getURL(), properties);
        assertNotNull(con);
    }
    @After
    public void closeConnection() throws Exception
    {
        assertNotNull(con);
        if( con.isClosed()) return;
        con.createStatement().executeUpdate("delete from default");
        con.close();
    }
    @Test
    public void testBadInstance() throws Exception
    {

        Cluster cluster = ((ProtocolImpl)((CBConnection) con).protocol).getCluster();

        String endpoint = "{\"cluster\":\"default\",\"name\":\"10.168.209.119\",\"queryEndpoint\":\"http://10.168.209.119:8093/query/service\"," +
                "\"adminEndpoint\":\"http://10.168.209.119:8093/admin\",\"options\":null}";

        ObjectMapper mapper = JsonFactory.create();
        Map <String,Object> instanceEndpoint = (Map)mapper.fromJson(endpoint);

        cluster.addEndPoint(instanceEndpoint);

        assertNotNull(con);
        Statement statement = con.createStatement();
        assertNotNull(statement);

        // there are only 4 endpoints we added 1 which is 5
        for (int i = 0; i++< 6;)
        {

            int inserted = statement.executeUpdate("INSERT INTO default  (KEY, VALUE) VALUES ( 'K" + i +"'," + i +")");
            assertEquals(1, inserted);
        }
    }
}
