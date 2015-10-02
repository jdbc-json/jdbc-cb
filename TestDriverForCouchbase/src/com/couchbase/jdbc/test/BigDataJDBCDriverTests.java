package com.couchbase.jdbc.test;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.couchbase.jdbc.ClusterInfo;
import com.couchbase.jdbc.ClusterSetupUtils;
import com.couchbase.jdbc.JDBCTestUtils;

@RunWith(JUnit4.class)
public class BigDataJDBCDriverTests {
	static ClusterInfo clusterInfo = null;
	
	@BeforeClass
	public static void initializeCluster() throws Exception
	{
		JDBCTestUtils.setConnection();
		String clusterConfigPath = "/tmp/config.json";
		BigDataJDBCDriverTests.clusterInfo = ClusterSetupUtils.readConfigFile(clusterConfigPath);
		ClusterSetupUtils.initializeCluster(BigDataJDBCDriverTests.clusterInfo);
		ClusterSetupUtils.createBuckets(BigDataJDBCDriverTests.clusterInfo);
		Thread.sleep(5000);
		JDBCTestUtils.createPrimaryIndexes(BigDataJDBCDriverTests.clusterInfo.bucketInformation.keySet());
	}
	
	@AfterClass
	public static void cleanupCluster() throws Exception
	{
		ClusterSetupUtils.deleteBuckets(BigDataJDBCDriverTests.clusterInfo);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testLargeDataSize() throws Exception
	{
		JDBCTestUtils.setConnection();
		JSONObject obj = new JSONObject();
		String deleteData = "delete from default";
		JDBCTestUtils.runQueryWithoutResult(deleteData);
		HashMap<String, Object> map = new HashMap<String, Object>();
		StringBuffer outputBuffer = new StringBuffer();
		for (int i = 0; i < 1000; i++){
		   outputBuffer.append("xx");
		}
		map.put("name", outputBuffer.toString());
		map.put("id", 12345);
		obj.putAll(map);
		JSONArray expectedArray = new JSONArray();
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1", obj);
		expectedArray.add(obj);
		JDBCTestUtils.insertData(objMap, "default");
		String query = "select * from default";
		JSONArray actualArray = JDBCTestUtils.runQueryAndExtractMap(query);
		assertEquals(expectedArray, actualArray);
	 }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testLargeNumberOfFields() throws Exception
	{
		JDBCTestUtils.setConnection();
		JSONObject obj = new JSONObject();
		String deleteData = "delete from default";
		JDBCTestUtils.runQueryWithoutResult(deleteData);
		HashMap<String, Object> map = new HashMap<String, Object>();
		for (int i = 1; i < 10000; i++){
			map.put(i+"", 12345);
		}
		obj.putAll(map);
		JSONArray expectedArray = new JSONArray();
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1", obj);
		expectedArray.add(obj);
		JDBCTestUtils.insertData(objMap, "default");
		String query = "select * from default";
		JSONArray actualArray = JDBCTestUtils.runQueryAndExtractMap(query);
		assertEquals(expectedArray, actualArray);
	 }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testLargeNumberOfObjects() throws Exception
	{
		JDBCTestUtils.setConnection();
		JSONObject obj = new JSONObject();
		String deleteData = "delete from default";
		JDBCTestUtils.runQueryWithoutResult(deleteData);
		JSONArray expectedArray = new JSONArray();
		for(int m=1;m<100;++m){
			HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
			for(int j=0;j<100;++j){
				HashMap<String, Object> map = new HashMap<String, Object>();
				StringBuffer outputBuffer = new StringBuffer();
				for (int i = 0; i < 1000; i++){
				   outputBuffer.append("xx");
				}
				map.put("name", outputBuffer.toString());
				map.put("id", 12345);
				obj.putAll(map);
				objMap.put(m+j+"", obj);
				expectedArray.add(obj);
			}
			JDBCTestUtils.bulkInsertData(objMap, "default");
			JDBCTestUtils.bulkUpsertData(objMap, "default");
		}
		String query = "select * from default";
		JSONArray actualArray = JDBCTestUtils.runQueryAndExtractMap(query);
		assertEquals(expectedArray, actualArray);
	 }
}
