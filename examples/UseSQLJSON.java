package com.couchbase.jdbc.examples;

import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;

import com.couchbase.jdbc.CBConnection;
import com.couchbase.jdbc.CBPreparedStatement;
import com.couchbase.jdbc.CBResultSet;
import com.couchbase.json.SQLJSON;

/*
 * This example shows how work with SQLJSON objects and arrays using the Couchbase JDBC driver.
 * It creates a value with two fields, received as parameters, one a JSON object and the
 * other a JSON array.
 * It then retrieves these two fields, and makes the accessible as a map and a list.
 */

public class UseSQLJSON {
    static String ConnectionURL = "jdbc:couchbase://localhost:8093";

    public static void main(String[] args) throws Exception {
        // Note the use of specialized versions of Connection and PreparedStatement,
        // so we can use to the SQLJSON access functions.
        CBConnection con = (CBConnection)DriverManager.getConnection(ConnectionURL);
        CBPreparedStatement ps = (CBPreparedStatement)con.prepareStatement("insert into default(key, value) values ('comp1', { 'obj': ?, 'arr': ?, 'type': 'sqljson-compound'})");
        
        // Set a JSON object parameter.
        HashMap<String, String> m = new HashMap<>();
        m.put("length", "234");
        m.put("width", "12");
        SQLJSON json = con.createSQLJSON();
        json.setMap(m);
        ps.setSQLJSON(1, json);
        
        // Set a JSON array parameter.
        SQLJSON arr = con.createSQLJSON();
        arr.setArray(Arrays.asList("one", "two", "three" ));
        ps.setSQLJSON(2, arr);
        
        int numUpdate = ps.executeUpdate();
        System.out.println("Created " + numUpdate);
        System.out.println();
        Thread.sleep(1000);
        
        // Retrieve the object.
        Statement stmt = con.createStatement();
        CBResultSet rs = (CBResultSet)stmt.executeQuery("select obj, arr from default where type = 'sqljson-compound'");
        rs.next();
        
        // Retrieve the JSON object field.
        System.out.println("RETRIEVED OBJECT FIELDS");
        SQLJSON objField = rs.getSQLJSON("obj");
        System.out.println("length: " + objField.getMap().get("length"));
        System.out.println("width: " + objField.getMap().get("width"));
        System.out.println();
        
        // Retrieve the JSON array field.
        System.out.println("RETRIEVED ARRAY ELEMENTS");
        SQLJSON arrField = rs.getSQLJSON("arr");
        for (Object o : arrField.getArray()) {
            System.out.println(o);
        }
        System.out.println();
        
        // Clean up.
        numUpdate = stmt.executeUpdate("delete from default where type = 'sqljson-compound'");
        System.out.println("Deleted " + numUpdate);
    }
}
