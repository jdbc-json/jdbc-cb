package examples;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/*
 * This example shows how work with JSON objects and arrays using the Couchbase JDBC driver.
 * It creates a value with two fields, received as parameters, one a JSON object and the
 * other a JSON array.
 * It then retrieves these two fields, and makes the accessible as a map and an array.
 */

public class CompoundElements {
    static String ConnectionURL = "jdbc:couchbase://localhost:8093";

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Connection con = DriverManager.getConnection(ConnectionURL);
        PreparedStatement ps = con.prepareStatement("insert into default(key, value) values ('comp1', { 'obj': ?, 'arr': ?, 'type': 'compound'})");
        
        // Set a JSON object parameter.
        HashMap<String, String> m = new HashMap<String, String>();
        m.put("length", "234");
        m.put("width", "12");
        ps.setObject(1, m);
        
        // Set a JSON array parameter.
        Object[] arrElems = new Object[]{ "one", "two", "three" };
        Array arr = con.createArrayOf(null, arrElems);
        ps.setArray(2, arr);
        
        int numUpdate = ps.executeUpdate();
        System.out.println("Created " + numUpdate);
        System.out.println();
        Thread.sleep(1000);
        
        // Retrieve the object.
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select obj, arr from default where type = 'compound'");
        rs.next();
        
        // Retrieve the JSON object field.
        System.out.println("RETRIEVED OBJECT FIELDS");
        Map<String, Object> objResult = (Map<String, Object>) rs.getObject("obj");
        System.out.println("length: " + objResult.get("length"));
        System.out.println("width: " + objResult.get("width"));
        System.out.println();
        
        // Retrieve the JSON array field.
        System.out.println("RETRIEVED ARRAY ELEMENTS");
        Array arrResult= rs.getArray("arr");
        Object[] elements = (Object[]) arrResult.getArray();
        for (Object o : elements) {
            System.out.println(o.toString());
        }
        System.out.println();
        
        // Clean up.
        numUpdate = stmt.executeUpdate("delete from default where type = 'compound'");
        System.out.println("Deleted " + numUpdate);
    }
}
