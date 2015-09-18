package com.couchbase.jdbc.test;
import com.couchbase.jdbc.test.TestResultAnalysis;
import org.junit.After;
import org.junit.AfterClass;

import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import junit.framework.TestCase;
import java.sql.*;
import static org.junit.Assert.*;
import com.couchbase.jdbc.test.JDBCTestUtils;

@RunWith(JUnit4.class)
public class RQGJDBCTests extends TestCase {
	
	public static ClusterInfo clusterInfo = null;
	
    @BeforeClass
    public static void initializeCluster() throws Exception
    {
    	// Create Cluster
    	String clusterConfigPath = "/tmp/config_rqg.json";
    	RQGJDBCTests.clusterInfo  = ClusterSetupUtils.readConfigFile(clusterConfigPath);
    	ClusterSetupUtils.initializeCluster(RQGJDBCTests.clusterInfo);
    }

    @After
    public void cleanupData() throws Exception
    {
    	ClusterSetupUtils.deleteBuckets(RQGJDBCTests.clusterInfo);
    	RQGJDBCTests.clusterInfo.resetBucketInformation();
    }
    
  
    public void testAggregateMethods() throws Exception
    {
    	String inputPath  = "/tmp/aggregate_datadump";
    	TestResultAnalysis analysis = JDBCTestUtils.runTestsWithAggregateFunctionQueries(RQGJDBCTests.clusterInfo, inputPath);
    	assertTrue(analysis.isTestPassing());
    }
 
    @Test
    public void testFields() throws Exception
    {
    	String inputPath  = "/tmp/field_datadump";
    	TestResultAnalysis analysis = JDBCTestUtils.runTestsWithFieldProjectionQueries(RQGJDBCTests.clusterInfo,inputPath);
    	assertTrue(analysis.isTestPassing());
    }
    
  
    public void testJoins() throws Exception
    {	
    	String inputPath  = "/tmp/joins_datadump";
    	TestResultAnalysis analysis = JDBCTestUtils.runTestsWithFieldProjectionQueries(RQGJDBCTests.clusterInfo, inputPath);
    	assertTrue(analysis.isTestPassing());
    }
    
}
