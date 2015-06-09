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
    }
    @After
    public void closeConnection() throws Exception
    {
        assertNotNull(con);
        con.close();
    }
    @Test
    public void select() throws Exception
    {
        con = DriverManager.getConnection(TestUtil.getURL(), TestUtil.getUser(), TestUtil.getPassword());
        Statement statement = con.createStatement();
        assertNotNull(statement);

        ResultSet rs = statement.executeQuery("SELECT children[0].fname AS cname "
                + "FROM contacts "
                + "WHERE age > 30 AND children IS NOT NULL");

        assertTrue(rs.next());


        rs = statement.executeQuery("SELECT fname, children "
                + " FROM contacts "
                + " WHERE ANY child IN contacts.children "
                + " SATISFIES child.age > 10  END ");

        rs = statement.executeQuery("SELECT fname, ARRAY child.fname FOR child IN c.children"
                + " END AS children_names"
                + " FROM contacts c"
                + " WHERE children IS NOT NULL");

        rs = statement.executeQuery("SELECT email, children"
                + " FROM contacts USE keys [ 'dave', 'earl', 'fred' ]"
                + " WHERE ANY child IN contacts.children SATISFIES child.age < 21  END");

        rs = statement.executeQuery("SELECT contacts.email, child"
                + " FROM contacts UNNEST contacts.children AS child");

        rs = statement.executeQuery("SELECT DISTINCT ccInfo.cardType"
                + " FROM customer");

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

        System.err.println(" executing query with product");
        ResultSet rs = statement.executeQuery("SELECT product.name, product.unitPrice"
        +" FROM product"
        +" WHERE ANY cat IN product.categories SATISFIES lower(cat) = 'golf' END"
        +" ORDER BY product.unitPrice DESC"
        +" LIMIT 5");


        System.err.println(" executing query with product 2");
        rs = statement.executeQuery("SELECT product.name, count(reviewID) AS  numReviews"
        +" FROM product UNNEST reviewList AS reviewID"
        +" GROUP BY product.productId, product.name"
        +" HAVING count(reviewID) >= 20"
        +" ORDER BY numReviews desc");

        System.err.println(" executing query with product 3");
        rs = statement.executeQuery("SELECT product.name, ARRAY_LENGTH(reviewList) as numReviews"
        +" FROM product"
        +" WHERE array_length(reviewList) >= 20"
        +" ORDER BY numReviews desc");

        System.err.println(" executing query with product 4");
        rs = statement.executeQuery("SELECT product.name, product.categories, reviews"
        +" FROM product"
        +" JOIN reviews ON KEYS product.reviewList"
        +" LIMIT 5");

        System.err.println(" executing query with product 5");
        rs = statement.executeQuery("SELECT product.name, AVG(reviews.rating) AS avg_rating"
        +" FROM reviews"
        +" JOIN product ON KEYS reviews.productId"
        +" GROUP BY product.name"
        +" ORDER BY avg_rating DESC"
        +" LIMIT 5");

        System.err.println(" executing query with product 6");
        rs = statement.executeQuery("SELECT product.name, reviews"
        +" FROM product"
        +" NEST reviews ON KEYS product.reviewList"
        +" LIMIT 5");

        System.err.println(" executing query with product 7");
        rs = statement.executeQuery("SELECT product.name,"
        +"  ARRAY review.rating FOR review IN reviews END AS ratings"
        +" FROM product"
        +" NEST reviews ON KEYS product.reviewList"
        +" LIMIT 5");

        System.err.println(" executing query with product 8");
        rs = statement.executeQuery("SELECT cat, product, reviews"
        +" FROM product UNNEST product.categories AS cat"
        +" JOIN reviews ON KEYS product.reviewList"
        +" LIMIT 5");

        rs = statement.executeQuery("SELECT cat, AVG(reviews.rating) as avg_rating,"
        +"  COUNT(reviews.rating) as num_ratings"
        +" FROM product UNNEST product.categories AS cat"
        +" JOIN reviews ON KEYS product.reviewList"
        +" WHERE substr(reviews.reviewedAt, 0, 4) = '2014'"
        +" GROUP BY cat"
        +" ORDER BY avg_rating desc");

        rs = statement.executeQuery("SELECT cat, SUM(lineItems.count * product.unitPrice) AS revenue,"
        +" AVG(lineItems.count * product.unitPrice) AS avg_revenue,"
        +" SUM(lineItems.count) AS units_sold"
        +" FROM purchases UNNEST lineItems"
        +" JOIN product ON KEYS lineItems.product UNNEST product.categories AS cat"
        +" WHERE purchases.purchasedAt IS NOT NULL"
        +" AND SUBSTR(purchases.purchasedAt, 0, 4) = '2013'"
        +" GROUP BY cat ORDER BY cat desc");

        rs = statement.executeQuery("SELECT p_sample.product.name,"
        +"  ARRAY_LENGTH(p_sample.product.reviewList) as numReviews"
        +" FROM (select p from product p limit 50) as p_sample"
        +" ORDER BY numReviews desc"
        +" LIMIT 5");

    }
}
