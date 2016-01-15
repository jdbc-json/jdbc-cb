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

The unit tests assume an instance of Couchbase is set up and accessible on
the local machine. The administrator and password should be "Administrator" and "password",
respectively. The "beer-sample" default data bucket (created at installation time)
should be present.

Run all the tests with this command:

    mvn test

Run a specific test with a command like this:

    mvn test -Dtest=com.couchbase.ReadOnlyQueryTest

##Prebuilt JARs

jars can be found at http://ec2-54-237-21-240.compute-1.amazonaws.com/index.html

##Usage

The driver url is jdbc:couchbase://\<host\>:\<port\>

Port should be 8093

