package com.couchbase.jdbc;

import com.couchbase.jdbc.util.Credentials;

import java.io.InputStream;
import java.util.*;

/**
 * Created by davec on 2015-02-22.
 */
public class TestUtil
{
    public static Properties environment=new Properties();
    public static ClusterInfo clusterInfo;

    static {
    	resetEnvironmentProperties("environment.properties");
    }
    
    public static void resetEnvironmentProperties(String propertPath){
    	environment=new Properties();
    	InputStream stream = null;
    	if (propertPath == null){
    		propertPath = "environment.properties";
    	}
    	if(propertPath != null){
    		 stream = ClassLoader.getSystemClassLoader().getResourceAsStream(propertPath);
    	 }
    	 try
         {
         	if( stream != null ){
         		environment.load( stream );
         	}
         }
         catch (Exception ex )
         {
             ex.printStackTrace(System.err);
         }
    	
    }
    
    public static String getConfig() { return environment.getProperty("couchbasedb.test.config", "config.json");}

    public static String getURL() { return environment.getProperty("couchbasedb.test.url", "jdbc:couchbase://127.0.0.1:8093");}

    public static String getSSLUrl() { return environment.getProperty("couchbasedb.test.sslurl", "jdbc:couchbase://127.0.0.1:18093");}

    public static String getBadURL() {return environment.getProperty("couchbasedb.test.url", "jdbc:couchbase://127.0.0.1:8093");}

    public static void initializeCluster(boolean createPrimaryIndex){
    	try{
		    	String clusterConfigPath = TestUtil.getConfig();
		    	TestUtil.clusterInfo =  ClusterSetupUtils.readConfigFile(clusterConfigPath);
		    	ClusterSetupUtils.initializeCluster(TestUtil.clusterInfo);
		    	System.out.println(TestUtil.clusterInfo.toString());
		    	ClusterSetupUtils.createBuckets(TestUtil.clusterInfo);
		    	Thread.sleep(5000);
		    	JDBCTestUtils.setConnection(TestUtil.clusterInfo.masterNodeInfo.getQueryServerURL());
	    	if(createPrimaryIndex){
	    		JDBCTestUtils.createPrimaryIndexes(TestUtil.clusterInfo.bucketInformation.keySet());
	    	}
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    }
    
    public static void destroyCluster(){
    	try{
    		ClusterSetupUtils.deleteBuckets(TestUtil.clusterInfo);
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    }
    
   
    
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
        return environment.getProperty("couchbasedb.test.user", "Adminisrator");
    }

    public static String getPassword() {
        return environment.getProperty("couchbasedb.test.password", "password");
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
