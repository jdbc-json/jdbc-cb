package com.couchbase.jdbc.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/*
 * This example shows
 * a) How to run a simple SELECT statement using the Couchbase JDBC driver.
 * b) How to retrieve scalar field values from the rows.
 */
public class SimpleSelects {
    static String ConnectionURL = "jdbc:couchbase://localhost:8093";

    public static void main(String[] args) throws Exception {
        Connection con = DriverManager.getConnection(ConnectionURL);
        Statement stmt = con.createStatement();
        
        ResultSet rs = stmt
                .executeQuery("select name, abv, (abv > 5) as strong from `beer-sample` where type = 'beer' limit 10");
        System.out.println("TEN BEERS");
        while (rs.next()) {
            String name = rs.getString("name");
            double abv = rs.getDouble("abv");
            boolean strong = rs.getBoolean("strong");
            System.out.println("name: " + name + ", abv: " + abv + ", strong: " + strong);
        }
        System.out.println();
        
        rs = stmt.executeQuery("select category, count(*) as num from `beer-sample` where type = 'beer' group by category");
        System.out.println("BEER CATEGORIES");
        while (rs.next()) {
            System.out.println(rs.getString("category") + ": " + rs.getInt("num"));
        }
    }
}
