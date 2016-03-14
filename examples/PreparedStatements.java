package com.couchbase.jdbc.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/*
 * This example shows how to use prepared statements and set parameters
 * through the Couchbase JDBC driver.
 */
public class PreparedStatements {
    static String ConnectionURL = "jdbc:couchbase://localhost:8093";

    public static void main(String[] args) throws Exception {
        Connection con = DriverManager.getConnection(ConnectionURL);
        PreparedStatement ps = con.prepareStatement("insert into default(key, value) values (?, [?, ?, ?, ?])");
        int numUpdates;
        
        // Insert first array.
        ps.setString(1, "ps1");
        ps.setInt(2,  33);
        ps.setDouble(3, 950.3);
        ps.setBoolean(4, false);
        ps.setString(5,  "ps");
        numUpdates = ps.executeUpdate();
        System.out.println("Inserted " + numUpdates);
        
        // Insert second array.
        ps.setString(1, "ps2");
        ps.setString(2,  "second");
        ps.setString(3, "second");
        ps.setString(4, "second");
        ps.setString(5,  "ps");
        numUpdates = ps.executeUpdate();
        System.out.println("Inserted " + numUpdates);

        // Insert third array.
        ps.setString(1, "ps3");
        ps.setInt(2,  3);
        ps.setInt(3, 3);
        ps.setInt(4, 3);
        ps.setString(5,  "ps");
        numUpdates = ps.executeUpdate();
        System.out.println("Inserted " + numUpdates);
        
        // Retrieve what we inserted.
        Thread.sleep(1000);  // Wait for the updates to propagate.
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select meta(d).id, * from default d where d[3] = 'ps'");
        while (rs.next()) {
            System.out.println("key: " + rs.getString("id") + ", value: " + rs.getString("d"));
        }
        
        // Clean up.
        numUpdates = stmt.executeUpdate("delete from default d where d[3] = 'ps'");
        System.out.println("Deleted " + numUpdates);
    }

}
