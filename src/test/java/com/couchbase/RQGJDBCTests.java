package com.couchbase;
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
import com.couchbase.jdbc.TestUtil;
import com.couchbase.jdbc.ClusterInfo;
import com.couchbase.jdbc.ClusterSetupUtils;
import com.couchbase.jdbc.JDBCTestUtils;
import com.couchbase.jdbc.TestResultAnalysis;

import junit.framework.TestCase;
import java.sql.*;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class RQGJDBCTests extends TestCase {
	
	@BeforeClass
	public static void initializeCluster() throws Exception
	{
		TestUtil.resetEnvironmentProperties(null);
		TestUtil.loadRQGProperties();
		TestUtil.initializeCluster(false);
	}
	
	@AfterClass
	public static void cleanupCluster() throws Exception
	{
		TestUtil.destroyCluster();
	}

    @After
    public void cleanupData() throws Exception
    {
    	TestUtil.destroyCluster();
    	TestUtil.clusterInfo.resetBucketInformation();
    }
    
    @Test
    public void testAggregateMethods() throws Exception
    {
    	String inputPath  = TestUtil.getRQGAggregateDir();
    	TestResultAnalysis analysis = JDBCTestUtils.runTestsWithAggregateFunctionQueries(TestUtil.clusterInfo, inputPath);
    	assertTrue(analysis.isTestPassing());
    }
 
    @Test
    public void testFields() throws Exception
    {
    	String inputPath  = TestUtil.getRQGFieldsDir();
    	TestResultAnalysis analysis = JDBCTestUtils.runTestsWithFieldProjectionQueries(TestUtil.clusterInfo,inputPath);
    	assertTrue(analysis.isTestPassing());
    }
    
    @Test
    public void testJoins() throws Exception
    {	
    	String inputPath  = TestUtil.getRQGJOINSDir();
    	TestResultAnalysis analysis = JDBCTestUtils.runTestsWithFieldProjectionQueries(TestUtil.clusterInfo, inputPath);
    	assertTrue(analysis.isTestPassing());
    }
    
}
