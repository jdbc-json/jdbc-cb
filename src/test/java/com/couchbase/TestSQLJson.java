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
import com.couchbase.json.SQLJSON;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by davec on 2015-07-10.
 */
public class TestSQLJson
{

    Connection con;

    @Before
    public void openConnection() throws Exception
    {
        con = DriverManager.getConnection(TestUtil.getURL(), TestUtil.getUser(), TestUtil.getPassword());
        assertNotNull(con);
        con.createStatement().executeUpdate("delete from test1");
        System.out.print("connection opened");
    }

    @After
    public void closeConnection() throws Exception
    {
        assertNotNull(con);

        try(Statement statement = con.createStatement())
        {
            statement.executeUpdate("delete from test1");
        }

        con.close();
    }

    @Test
    public void getSqlJson() throws Exception
    {
        String query = "SELECT * FROM customer limit 10";

        try (Statement stmt= con.createStatement())
        {
            CBResultSet rs = (CBResultSet)stmt.executeQuery(query);
            assertNotNull(rs);

            while (rs.next())
            {
                SQLJSON sqljson = rs.getSQLJSON(1);
                Map map = sqljson.parse();
                assertNotNull(map);
            }
        }
    }
    @Test
    public void setSqlJson() throws Exception
    {
        String json = "{\n" +
                "  \"emailAddress\": \"jakayla@crooks.info\",\n" +
                "  \"type\": \"customer\",\n" +
                "  \"dateLastActive\": \"2014-05-06T15:52:14Z\",\n" +
                "  \"firstName\": \"Darrin\",\n" +
                "  \"phoneNumber\": \"497-854-2229 x000\",\n" +
                "  \"postalCode\": \"45603-9112\",\n" +
                "  \"lastName\": \"Ortiz\",\n" +
                "  \"ccInfo\": {\n" +
                "    \"cardNumber\": \"1234-2121-1221-1211\",\n" +
                "    \"cardType\": \"discover\",\n" +
                "    \"cardExpiry\": \"2012-11-12\"\n" +
                "  },\n" +
                "  \"dateAdded\": \"2013-06-10T15:52:14Z\",\n" +
                "  \"state\": \"IN\",\n" +
                "  \"customerId\": \"customer10\"\n" +
                "}";

        String query = "insert into test1 (key,value) values (?,?)";

        SQLJSON sqljson = ((CBConnection)con).createSQLJSON();
        sqljson.setString( json );
        try (PreparedStatement pstmt = con.prepareStatement(query))
        {
            pstmt.setString(1,"customer1");
            ((CBPreparedStatement)pstmt).setSQLJSON(2,sqljson);
            assertEquals(1,pstmt.executeUpdate());
        }
    }
}
