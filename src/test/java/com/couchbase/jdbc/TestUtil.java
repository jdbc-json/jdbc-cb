package com.couchbase.jdbc;

import java.util.Properties;

/**
 * Created by davec on 2015-02-22.
 */
public class TestUtil
{
    public static String getURL() { return System.getProperty("couchbasedb.test.url", "jdbc:couchbase://ec2-54-146-69-136.compute-1.amazonaws.com:8093");}

    public static String getBadURL() {return System.getProperty("couchbasedb.test.url", "jdbc:couchbase://ec2-54-146-69-136.compute-1.amazonaws.com:8093");}

    public static String getServer() {
        return System.getProperty("couchbasedb.test.server", "ec2-54-146-69-136.compute-1.amazonaws.com");
    }

    public static String getPort() {
        return System.getProperty("couchbasedb.test.port", "8093");
    }

    public static String getDatabase() {
        return System.getProperty("couchbasedb.test.db", "test");
    }

    public static Properties getProperties() {

        Properties props = new Properties();

        props.setProperty("user", getUser());
        props.setProperty("password", getPassword());

        return props;
    }

    public static String getUser() {
        return System.getProperty("couchbasedb.test.user", "pgjdbc");
    }

    public static String getPassword() {
        return System.getProperty("couchbasedb.test.password", "test");
    }


}
