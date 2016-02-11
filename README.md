#Couchbase JDBC Driver

## Setup and Use

These instructions assume you are conversant with the Java ecosystem, and therefore
know your way around Eclipse and Maven. You need to be running at least Java 7.

Install Couchbase on your local machine. You should be able to access the administration interface at http://localhost:8091. The *beer-sample* sample data bucket should be present and accessible without a password.

Create a new Maven project in Eclipse. Add this dependency to the pom.xml file:

    <dependencies>
      <dependency>
        <groupId>com.github.jdbc-json</groupId>
        <artifactId>jdbc-cb</artifactId>
        <version>0.4.0</version>
      </dependency>
    </dependencies>

Add a new Java class to the src/main/java directory of the project:

    package testing;
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

Run the new class as a Java Application. You should see this output:

    12:35:18.397 [main] INFO  com.couchbase.CBDriver - Constructor called
    12:35:18.401 [main] INFO  com.couchbase.CBDriver - Constructor called
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

##Build instructions

### Requirements:
* java 1.8
* git
* maven

Steps:

    git clone https://github.com/jdbc-json/jdbc-cb
    cd jdbc-cb
    mvn -Dmaven.test.skip=true package

the jar file will be in the target directory.

##Test instructions

The unit tests assume an instance of Couchbase Enterprise Edition is set up and accessible on
the local machine. The administrator and password should be "Administrator" and "password",
respectively. The *beer-sample* and *default* data buckets
should be present. The *default* bucket is always present; *beer-sample* is created on request at installation time.

The beer-sample and default buckets must be indexed for the tests to run correctly.
You can index them by running these two commands in the
[CBQ shell](http://developer.couchbase.com/documentation/server/4.0/n1ql/n1ql-intro/cbq.html).

    create primary index on default;
    create primary index on `beer-sample`;
    
Note the back-ticks in the second line.

If the Couchbase instance is Community Edition, the SSLConnectionTest will fail because
Community Edition instances are not accessible over SSL.

Run all the tests with this command:

    mvn test

Run a specific test with a command like this:

    mvn test -Dtest=com.couchbase.ReadOnlyQueryTest

##Usage

The Maven coordinates of the driver is

    <dependency>
        <groupId>com.github.jdbc-json</groupId>
        <artifactId>jdbc-cb</artifactId>
        <version>0.4.0</version>
    </dependency>

The driver url is jdbc:couchbase://\<host\>:\<port\>

Port should be 8093

