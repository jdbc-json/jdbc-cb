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

import com.couchbase.json.SQLJSON;

import java.sql.*;

/**
 * Created by davec on 2015-10-15.
 */
public class Issue16405 {
    //{ 'a1': 'Object' } as c6
    static String ConnectionURL = "jdbc:couchbase://54.237.32.30:8093";

    public static void main(String[] args) throws SQLException {

        String query = "SELECT  1  as a1";

        try (Connection con = DriverManager.getConnection(ConnectionURL)) {

            try (Statement stmt = con.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(query)) {
                    rs.next();
                    SQLJSON jsonVal = ((CBResultSet)rs).getSQLJSON(1);
                    String one = jsonVal.getString();
                    System.out.println("This should equal 1 --> " + one);
                }
            }
        }
    }
}
