package com.couchbase.jdbc.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import com.couchbase.jdbc.util.Credentials;

/*
 * This example shows how to provide per-bucket credentials (passwords)
 * through the Couchbase JDBC driver.
 * 
 * Initially, this code will not work. It requires some setup.
 * First create two buckets, rb1 and rb2. Then use the CBQ tool to create primary indexes on both
 * buckets. Finally add passwords rb1p and rb2p on the two buckets.
 * The code will then run correctly.
 * 
 * Instead of supplying both per-bucket passwords, you can supply the Administrator
 * and admin password instead. That overrides the per-bucket passwords.
 */
public class UseCredentials {
    static String ConnectionURL = "jdbc:couchbase://localhost:8093";

    public static void main(String[] args) throws Exception {        
        Credentials cred = new Credentials();
        cred.add("rb1", "rb1p");
        cred.add("rb2", "rb2p");
        // cred.add("Administrator", "password");  // Alternate credential.
        
        Properties prop = new Properties();
        prop.setProperty("credentials", cred.toString());

        Connection con = DriverManager.getConnection(ConnectionURL, prop);
        Statement stmt = con.createStatement();
        stmt.executeQuery("select count(*) from rb1 union select count(*) from rb2");
        System.out.println("Success.");
    }

}
