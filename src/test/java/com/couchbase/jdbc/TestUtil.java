package com.couchbase.jdbc;

import com.couchbase.jdbc.util.Credentials;

import java.io.InputStream;
import java.util.*;

/**
 * Created by davec on 2015-02-22.
 */
public class TestUtil
{
    static Properties environment=new Properties();

    static {
        InputStream stream = ClassLoader.getSystemClassLoader().getResourceAsStream( "environment.properties");
        try

        {
            environment.load( stream );
        }
        catch (Exception ex )
        {
            ex.printStackTrace(System.err);
        }
    }
        public static String getURL() { return environment.getProperty("couchbasedb.test.url", "jdbc:couchbase://ec2-54-146-69-136.compute-1.amazonaws.com:8093");}

    public static String getBadURL() {return environment.getProperty("couchbasedb.test.url", "jdbc:couchbase://ec2-54-146-69-136.compute-1.amazonaws.com:8093");}

    public static String getServer() {
        return System.getProperty("couchbasedb.test.server", "ec2-54-146-69-136.compute-1.amazonaws.com");
    }

    public static String getPort() {
        return environment.getProperty("couchbasedb.test.port", "8093");
    }

    public static String getDatabase() {
        return environment.getProperty("couchbasedb.test.db", "test");
    }

    public static Properties getProperties() {

        Properties props = new Properties();

        props.setProperty("user", getUser());
        props.setProperty("password", getPassword());

        return props;
    }

    public static String getUser() {
        return environment.getProperty("couchbasedb.test.user", "pgjdbc");
    }

    public static String getPassword() {
        return environment.getProperty("couchbasedb.test.password", "test");
    }

    public static Credentials getCredentials() {
        Credentials credentials = new Credentials();
        String auth = environment.getProperty("couchbasedb.test.auth","");
        String [] parts = auth.split(",");
        for (String part : parts )
        {
            String []creds = part.split(":");
            credentials.add(creds[0],creds[1]);
        }
        return credentials;

    }
    static String []resultSetGetters = {
            "getArray","getAsciiStream",
            "getString","getBigDecimal",
            "getBinaryStream",
            "getBoolean", "getByte",
            "getBytes", "getCharacterStream",
            "getDate",
            "getDouble","getInt",
            "getLong", "getObject",
            "getShort",
            "getString", "getTime",
            "getTimestamp", "getUnicodeStream",
            "getURL"
    };
    static String []notImplemented = {
            "getClob","getBlob"

    } ;
    public static List<String> getSuppportedResultSetGetters()
    {
        return Arrays.asList(resultSetGetters);
    }

}
