#Couchbase JDBC Driver

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

##Prebuilt JARs

jars can be found at http://ec2-54-237-21-240.compute-1.amazonaws.com/index.html

##Usage

The driver url is jdbc:couchbase://\<host\>:\<port\>

Port should be 8093

