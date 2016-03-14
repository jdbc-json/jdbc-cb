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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.*;

/**
 * Created by davec on 2015-08-11.
 */
@RunWith(JUnit4.class)
public class Test16017 extends CouchBaseTestCase
{
    @Test
    public void testValues() throws Exception
    {
        Statement statement = con.createStatement();
        assertNotNull(statement);
        statement.executeUpdate("insert into default (key,value) values ('1',{\n" +
                "                    \"bool_field1\": false,\n" +
                "                    \"char_field1\": \"N\",\n" +
                "                    \"datetime_field1\": \"2009-05-19 00:00:00\",\n" +
                "                    \"decimal_field1\": 4763,\n" +
                "                    \"int_field1\": 1790,\n" +
                "                    \"primary_key_id\": \"1\",\n" +
                "                    \"varchar_field1\": \"EpTpDkFseV\"\n" +
                "                })");
        ResultSet rs = statement.executeQuery("select * from default");
        String columnName;
        Object columnValue;
        while (rs.next()) {
            int total_rows = rs.getMetaData().getColumnCount();
            for (int i = 0; i < total_rows; i++) {
                columnName = rs.getMetaData().getColumnLabel(i + 1).toLowerCase();
                columnValue = rs.getObject(i + 1);

                if (columnValue == null){
                    columnValue = "null";
                }
                System.out.println(columnName +':' + columnValue);

            }

        }

    }


}
