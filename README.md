# Couchbase JDBC Driver

This project is a JDBC driver for accessing the Couchbase NoSQL database. Couchbase stores data as JSON documents.
The N1QL query language is based on SQL, but designed for querying JSON data sources, such as Couchbase.
This driver lets the user run N1QL queries against Couchbase, and retrieve JSON data through the standard
JDBC interfaces. 

Both simple JSON values (string/numbers/booleans) and compound JSON values (objects/arrays) are supported.
Both types of values can be retrieved as fields and supplied as parameters.

Users who do not have a pre-existing investment in JDBC but still want to access Couchbase through Java
may find the [Couchbase Java SDK](http://developer.couchbase.com/documentation/server/current/sdk/java/start-using-sdk.html)
a more convenient solution.

## Brief Setup Instructions

The Maven coordinates of the driver are 

      <dependency>
        <groupId>com.couchbase.jdbc</groupId>
        <artifactId>jdbc-n1ql</artifactId>
        <version>1.0-BETA</version>
      </dependency>

The driver enables communication with Couchbase through the standard JDBC interfaces. Use connect string `jdbc:couchbase://localhost:8093` to connect to the Couchbase instance on your local machine.

If you have the *beer-sample* sample bucket installed, this query returns a manageable set of ten rows with two fields per row:

    select name, code from `beer-sample` where city = 'San Francisco' and type = 'brewery'

To run the query, you will need to create a primary index on *beer-sample*.

## Detailed Setup Instructions

These instructions assume you are conversant with the Java ecosystem, and therefore
know your way around Maven. Use Maven 3 and Java 8. 

Install Couchbase on your local machine. Instructions are [here](http://developer.couchbase.com/documentation/server/current/getting-started/installing.html).

Go to to Administration Console at *http://localhost:8091* and log in using the Administrator password you supplied during installation. In the *Settings* tab you will find the *Sample Buckets* section, with a set of available sample buckets. Check the *beer-sample* box to create this set of sample data.

CBQ is the tool for running N1QL queries against Couchbase. Instructions for running it are [here](http://developer.couchbase.com/documentation/server/current/n1ql/n1ql-intro/cbq.html). Create a primary index on the *beer-sample* bucket by running this command in CBQ:

    create primary index on `beer-sample`;
    
Create a new Maven project. Add this dependency to the *pom.xml* file, which should already exist in your new project:

    <dependencies>
      <dependency>
        <groupId>com.couchbase.jdbc</groupId>
        <artifactId>jdbc-n1ql</artifactId>
        <version>1.0-BETA/version>
      </dependency>
    </dependencies>
    
Add a new Java class to the src/main/java/trial directory of the project:

    package com.couchbase.jdbc.examples;
    import java.sql.Connection;
    import java.sql.DriverManager;
    import java.sql.ResultSet;
    import java.sql.Statement;
    public class SimpleVerification {
        static String ConnectionURL = "jdbc:couchbase://localhost:8093";
        public static void main(String[] args) throws Exception {
            Connection con = DriverManager.getConnection(ConnectionURL);
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("select name,code from `beer-sample` where city = 'San Francisco' and type = 'brewery'");
            while (rs.next()) {
                String name = rs.getString("name");
                String code = rs.getString("code");
                System.out.println("name: " + name + ", code: " + code);
            }
        }
    }

Run the new class. You should see this output:

    12:35:19.121 [main] DEBUG com.couchbase.CBResultSet - Loaded result set
    name: 21st Amendment Brewery Cafe, code: 94107
    name: Anchor Brewing, code: 94107
    name: Beach Chalet Brewery, code: 94121
    name: Big Bang Brewery (Closed), code: 94107
    name: Golden Gate Park Brewery, code: 
    name: Magnolia Pub and Brewery, code: 94117
    name: Shmaltz Brewing Company, code: 
    name: Shmaltz Enterprises, code: 94110
    name: Speakeasy Ales and Lagers, code: 94124
    name: ThirstyBear Brewing, code: 94105
    
The */examples* directory contains additional example files, showing how to use the driver.

## Additional Usage Notes

The connection string, used in the `DriverManager.getConnection()` call, should be of the form  `jdbc:couchbase://<host>:<port>`.

If you are working with an instance of Couchbase on your local machine, use the string `jdbc:couchbase://localhost:8093`. 

If you are working with a Couchbase cluster, the host should be the name or ip address of a Couchbase node running the Query service.
The driver will then distribute the queries around the nodes of the cluster in round-robin fashion. You can check which nodes are
running the Query service on the "Server Nodes" tab of the Couchbase Admin Console. The "Services" column shows which nodes are
running which service. The port is the port of the query service; 8093 by default.

Couchbase supports access over SSL-protected connections, but only Enterprise Edition supports this, not Community Edition.
To use SSL, connect on the 18093 port rather than the standard 8093 port. For a more detailed example of how to use SSL, consult
the SSL connection test at *src/test/java/com/couchbase/jdbc/SSLConnectionTest.java*.

## Building the Driver from Source

You need to have Git, Maven 3 and Java 8 installed on your machine.

Run the following commands:

    git clone https://github.com/jdbc-json/jdbc-cb
    cd jdbc-cb
    mvn -Dmaven.test.skip=true package

The JAR file will be in the */target* directory.

By default, `mvn package` runs the unit tests before creating the JAR. 
These directions specifically omit running the unit tests, because the tests require additional setup, as explained in the next section. 
After the setup is complete, you can run `mvn package` without the extra flag.

## Running the Unit Tests

(Assuming you have downloaded and built the driver from source, as described above.)

The unit tests assume an instance of Couchbase Enterprise Edition is set up and accessible on
the local machine. If the Couchbase instance is Community Edition, the SSLConnectionTest will fail because
Community Edition instances are not accessible over SSL. 
The administrator and password should be "Administrator" and "password",
respectively. The *beer-sample* and *default* data buckets
should be present. The *default* bucket is always present; *beer-sample* is created on request at installation time.

The beer-sample and default buckets must be indexed for the tests to run correctly.
You can index them by running these two commands in the CBQ:

    create primary index on default;
    create primary index on `beer-sample`;
    
Note the back-ticks in the second line.

Run all the tests with this command:

    mvn test

Run a specific test with a command like this:

    mvn test -Dtest=com.couchbase.jdbc.PreparedStatementTest
