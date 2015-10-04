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
import com.couchbase.jdbc.util.Credentials;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Created by davec on 2015-05-06.
 */
@RunWith(JUnit4.class)
public class ReadOnlyQueryTest extends TestCase
{
    Connection con;
    @Before
    public void openConnection() throws Exception
    {
    	TestUtil.resetEnvironmentProperties(null);
    	TestUtil.initializeCluster(true);
    }
    @After
    public void closeConnection() throws Exception
    {
        assertNotNull(con);
        con.close();
    }

    @Test
    public void testAuthentication() throws Exception
    {

        Credentials credentials = TestUtil.getCredentials();

        Properties props = new Properties();
        props.setProperty("credentials", credentials.toString());

        con = DriverManager.getConnection(TestUtil.getURL(), props);
        Statement statement = con.createStatement();
        assertNotNull(statement);

        System.err.println(" executing query with default");

        System.err.println(" executing query with default 2");
        ResultSet rs = statement.executeQuery("SELECT default.name, count(reviewID) AS  numReviews"
        +" FROM default UNNEST reviewList AS reviewID"
        +" GROUP BY default.defaultId, default.name"
        +" HAVING count(reviewID) >= 20"
        +" ORDER BY numReviews desc");

        System.err.println(" executing query with default 3");
        rs = statement.executeQuery("SELECT default.name, ARRAY_LENGTH(reviewList) as numReviews"
        +" FROM default"
        +" WHERE array_length(reviewList) >= 20"
        +" ORDER BY numReviews desc");

        System.err.println(" executing query with default 4");
        rs = statement.executeQuery("SELECT default.name, default.categories, reviews"
        +" FROM default"
        +" JOIN reviews ON KEYS default.reviewList"
        +" LIMIT 5");

        System.err.println(" executing query with default 5");
        rs = statement.executeQuery("SELECT default.name, AVG(reviews.rating) AS avg_rating"
        +" FROM reviews"
        +" JOIN default ON KEYS reviews.defaultId"
        +" GROUP BY default.name"
        +" ORDER BY avg_rating DESC"
        +" LIMIT 5");

        System.err.println(" executing query with default 6");
        rs = statement.executeQuery("SELECT default.name, reviews"
        +" FROM default"
        +" NEST reviews ON KEYS default.reviewList"
        +" LIMIT 5");

        System.err.println(" executing query with default 7");
        rs = statement.executeQuery("SELECT default.name,"
        +"  ARRAY review.rating FOR review IN reviews END AS ratings"
        +" FROM default"
        +" NEST reviews ON KEYS default.reviewList"
        +" LIMIT 5");

        System.err.println(" executing query with default 8");
        rs = statement.executeQuery("SELECT cat, default, reviews"
        +" FROM default UNNEST default.categories AS cat"
        +" JOIN reviews ON KEYS default.reviewList"
        +" LIMIT 5");

        rs = statement.executeQuery("SELECT cat, AVG(reviews.rating) as avg_rating,"
        +"  COUNT(reviews.rating) as num_ratings"
        +" FROM default UNNEST default.categories AS cat"
        +" JOIN reviews ON KEYS default.reviewList"
        +" WHERE substr(reviews.reviewedAt, 0, 4) = '2014'"
        +" GROUP BY cat"
        +" ORDER BY avg_rating desc");

        rs = statement.executeQuery("SELECT cat, SUM(lineItems.count * default.unitPrice) AS revenue,"
        +" AVG(lineItems.count * default.unitPrice) AS avg_revenue,"
        +" SUM(lineItems.count) AS units_sold"
        +" FROM purchases UNNEST lineItems"
        +" JOIN default ON KEYS lineItems.default UNNEST default.categories AS cat"
        +" WHERE purchases.purchasedAt IS NOT NULL"
        +" AND SUBSTR(purchases.purchasedAt, 0, 4) = '2013'"
        +" GROUP BY cat ORDER BY cat desc");

        rs = statement.executeQuery("SELECT p_sample.default.name,"
        +"  ARRAY_LENGTH(p_sample.default.reviewList) as numReviews"
        +" FROM (select p from default p limit 50) as p_sample"
        +" ORDER BY numReviews desc"
        +" LIMIT 5");

    }
}
