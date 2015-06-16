#Couchbase JDBC Driver

##Build instructions

### Requirements:
* java 1.8
* git
* maven

Steps:

    git clone git@github.com:couchbaselabs/jdbc_n1ql.git
    cd jdbc_n1ql
    mvn -Dmaven.test.skip=true package

the jar file will be in the target directory.

Tests can be run using

    mvn test -Dtest=com.couchbase.ReadOnlyQueryTest

for instance

jars can be found at http://ec2-54-237-21-240.compute-1.amazonaws.com/index.html

Usage:

The driver url is jdbc:couchbase://<host>:<port>

Port should be 8093

