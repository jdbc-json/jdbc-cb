package com.couchbase.jdbc;

import com.couchbase.json.SQLJSON;
import com.couchbase.jdbc.CBResultSet;

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

@RunWith(JUnit4.class)
public class ClusterAwareTests {
	
	@BeforeClass
	public static void initializeCluster() throws Exception
	{
		TestUtil.resetEnvironmentProperties(null);
		TestUtil.setRebalancePermission();
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
