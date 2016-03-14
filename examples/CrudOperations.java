package com.couchbase.jdbc.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/*
 * This example shows how to insert, update, select, and delete values
 * using the Couchbase JDBC driver.
 * 
 * This example adds, modifies, retrieves, and deletes a value from the "default" bucket.
 */
public class CrudOperations {
    
    static String ConnectionURL = "jdbc:couchbase://localhost:8093";

    public static void main(String[] args) throws Exception {

        Connection con = DriverManager.getConnection(ConnectionURL);
        Statement stmt = con.createStatement();
        
        int numUpdates;
        
        // Create a value.
        numUpdates = stmt.executeUpdate("insert into default (key, value) values ('crud-test1', { 'type': 'crud', 'num': 55 }), ('crud-test2', { 'type': 'crud', 'num': 65 }) ");
        System.out.println("Created " + numUpdates + " values.");
        Thread.sleep(1000); // Wait for the update to propagate.
        
        // Modify a value.
        numUpdates = stmt.executeUpdate("update default set name = 'apple' where type = 'crud' and num = 55");
        System.out.println("Modified " + numUpdates + " values.");
        Thread.sleep(1000); // Wait for the update to propagate.


        // Retrieve the values.
        ResultSet rs = stmt.executeQuery("select * from default where type = 'crud'");
        while (rs.next()) {
            System.out.println("Value: " + rs.getString(1));
        }
        
        // Delete the values.
        numUpdates = stmt.executeUpdate("delete from default where type = 'crud'");
        System.out.println("Deleted " + numUpdates + " values.");
    }
}
