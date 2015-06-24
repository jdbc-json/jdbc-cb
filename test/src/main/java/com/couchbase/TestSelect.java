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

import com.couchbase.util.Database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by davec on 2015-06-23.
 */
public class TestSelect
{
    public static void main(String []args)
    {
        try
        {
            new TestSelect().testSelect();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }

    }
    public boolean testSelect() throws SQLException
    {
       String queryAll = "SELECT * FROM `beer1-sample`";
       String queryBeer = "SELECT * FROM `beer-sample` where type = \"beer\"";
       String queryBrewery = "SELECT * FROM `beer-sample` where type = \"brewery\"";

       int allCount = countResults(queryAll);
       int beerCount = countResults(queryBeer);
       int breweryCount = countResults(queryBrewery);

       System.out.printf("Total count = %d, beer count = %d, brewery Count = %d\n",
               allCount, beerCount, breweryCount);
       return true;
    }

    public int countResults(String query) throws SQLException
    {
        int count=0;

        try (Connection con=Database.getConnection("54.237.32.30", 8093))
        {
            try (Statement stmt= con.createStatement())
            {
                ResultSet rs = stmt.executeQuery(query);
                while (rs.next())
                {
                    count++;
                }
            }
        }
        return count;
    }


}
