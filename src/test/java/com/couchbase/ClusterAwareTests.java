package com.couchbase;

import com.couchbase.json.SQLJSON;
import com.couchbase.CBResultSet;
import com.couchbase.jdbc.ClusterInfo;
import com.couchbase.jdbc.ClusterSetupUtils;
import com.couchbase.jdbc.JDBCTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import static org.junit.Assert.*;
import java.util.Map;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import com.couchbase.jdbc.TestUtil;

@RunWith(JUnit4.class)
public class ClusterAwareTests {
	
	@BeforeClass
	public static void initializeCluster() throws Exception
	{
		TestUtil.resetEnvironmentProperties(null);
		TestUtil.initializeCluster(true);
	}
	
	@AfterClass
	public static void cleanupCluster() throws Exception
	{
		TestUtil.destroyCluster();
	}
	
	@After
	public void cleanupBucket() throws Exception
	{
		JDBCTestUtils.deleteDataFromBucket("default");
		Thread.sleep(1000);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRemoveService() throws Exception
	{
		
	 }
	
	
}
