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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import com.couchbase.jdbc.TestUtil;

@RunWith(JUnit4.class)
public class ProjectionJDBCDriverTests {

	
	
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
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSimpleData() throws Exception
	{
		JDBCTestUtils.setConnection();
		JSONObject obj = new JSONObject();
		String deleteData = "delete from default";
		JDBCTestUtils.runQueryWithoutResult(deleteData);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("name", "test_name");
		map.put("specialchars", "()*&^%$!@{}{}:\"\\\';::");
		map.put("id", 12345);
		map.put("double", 12345.333);
		map.put("boolean", true);
		map.put("data_time", "2001-01-01 01:01:01.00");
		obj.putAll(map);
		JSONArray expectedArray = new JSONArray();
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_testSimpleData", obj);
		expectedArray.add(obj);
		JDBCTestUtils.insertData(objMap, "default");
		String query = "select * from default";
		JSONArray actualArray = JDBCTestUtils.runQueryAndExtractMap(query);
		assertEquals(expectedArray, actualArray);
	 }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testNulls() throws Exception
	{
		JDBCTestUtils.setConnection();
		JSONObject obj = new JSONObject();
		String deleteData = "delete from default";
		JDBCTestUtils.runQueryWithoutResult(deleteData);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("name", null);
		obj.putAll(map);
		JSONArray expectedArray = new JSONArray();
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_testNulls", obj);
		expectedArray.add(obj);
		JDBCTestUtils.insertData(objMap, "default");
		String query = "select * from default";
		JSONArray actualArray = JDBCTestUtils.runQueryAndExtractMap(query);
		assertEquals(expectedArray, actualArray);
	 }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testSimpleDataWithNull() throws Exception
	{
		JDBCTestUtils.setConnection();
		JSONObject obj = new JSONObject();
		String deleteData = "delete from default";
		JDBCTestUtils.runQueryWithoutResult(deleteData);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("name", null);
		obj.putAll(map);
		JSONArray expectedArray = new JSONArray();
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_testSimpleDataWithNull", obj);
		expectedArray.add(obj);
		JDBCTestUtils.insertData(objMap, "default");
		String query = "select * from default";
		JSONArray actualArray = JDBCTestUtils.runQueryAndExtractMap(query);
		assertEquals(expectedArray, actualArray);
	 }
	
	@SuppressWarnings({ "unchecked", "deprecation", "resource" })
	@Test
	public void testSimpleDataDifferentDataTypesAsFields() throws Exception
	{
		JDBCTestUtils.setConnection();
		JSONObject obj = new JSONObject();
		String deleteData = "delete from default";
		JDBCTestUtils.runQueryWithoutResult(deleteData);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("name", "test_name");
		map.put("specialchars", "()*&^%$!@{}{}:\"\\\';::");
		map.put("int", 12345);
		map.put("double", 12345.333);
		map.put("data_time", "2001-01-01 01:01:01.00");
		obj.putAll(map);
		JSONArray expectedArray = new JSONArray();
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_testSimpleDataDifferentDataTypesAsFields", obj);
		expectedArray.add(obj);
		JDBCTestUtils.insertData(objMap, "default");
		String query = "select name,specialchars,int,double,data_time from default";
		JDBCTestUtils.setConnection();
		 
        try ( Connection con = JDBCTestUtils.con)
        {
            try (Statement stmt = con.createStatement())
            {
                try (ResultSet rs = stmt.executeQuery(query))
                {
                	while(rs.next()){
                		CBResultSet cbrs = (CBResultSet) rs;
                		java.sql.ResultSetMetaData meta = cbrs.getMetaData();
                		System.out.println(" number of columns "+meta.getColumnCount());
		                SQLJSON jsonVal1 = cbrs.getSQLJSON("name");
		                assertEquals(2000, jsonVal1.getJDBCType());
		                assertEquals("test_name", jsonVal1.getString());
		                jsonVal1 = cbrs.getSQLJSON("specialchars");
		                assertEquals(2000, jsonVal1.getJDBCType());
		                assertEquals("()*&^%$!@{}{}:\"\\\';::", jsonVal1.getString());
		                jsonVal1 = cbrs.getSQLJSON("int");
		                assertEquals(Types.INTEGER, jsonVal1.getJDBCType());
		                assertEquals(1, jsonVal1.getInt());
		                jsonVal1 = cbrs.getSQLJSON("double");
		                assertEquals(Types.DOUBLE, jsonVal1.getJDBCType());
		                assertEquals(12345.333, jsonVal1.getDouble());
                	}
                }
            }
            
        }
	 }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testNestedOperator() throws Exception
	{
		JDBCTestUtils.setConnection();
		JSONObject obj = new JSONObject();
		JSONObject nestedobj = new JSONObject();
		String deleteData = "delete from default";
		JDBCTestUtils.runQueryWithoutResult(deleteData);
		HashMap<String, Object> map = new HashMap<String, Object>();
		nestedobj.put("nesteId", "1");
		map.put("name", "test_name");
		map.put("specialchars", "()*&^%$!@{}{}:\"\\\';::");
		map.put("id", 12345);
		map.put("double", 12345.333);
		map.put("data_time", "2001-01-01 01:01:01.00");
		obj.putAll(map);
		obj.put("nested", nestedobj);
		JSONArray expectedArray = new JSONArray();
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_testNestedOperator", obj);
		expectedArray.add(nestedobj);
		JDBCTestUtils.insertData(objMap, "default");
		String query = "select default.nested.* from default";
		JSONArray actualArray = JDBCTestUtils.runQueryAndExtractMap(query);
		assertEquals(expectedArray, actualArray);
	 }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testNoResult() throws Exception
	{
		JDBCTestUtils.setConnection();
		JSONObject obj = new JSONObject();
		String deleteData = "delete from default";
		JDBCTestUtils.runQueryWithoutResult(deleteData);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("name", "test_name");
		obj.putAll(map);
		JSONArray expectedArray = new JSONArray();
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_testNestedOperator", obj);
		JDBCTestUtils.insertData(objMap, "default");
		String query = "select * from default where x=1234";
		JSONArray actualArray = JDBCTestUtils.runQueryAndExtractMap(query);
		assertEquals(expectedArray, actualArray);
	 }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testNoResultWithBucketEmpty() throws Exception
	{
		JDBCTestUtils.setConnection();
		String deleteData = "delete from default";
		JDBCTestUtils.runQueryWithoutResult(deleteData);
		JSONArray expectedArray = new JSONArray();
		String query = "select * from default where x=1234";
		JSONArray actualArray = JDBCTestUtils.runQueryAndExtractMap(query);
		assertEquals(expectedArray, actualArray);
	 }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testQueryAggregate() throws Exception
	{
		JDBCTestUtils.setConnection();
		JSONObject obj = new JSONObject();
		String deleteData = "delete from default";
		JDBCTestUtils.runQueryWithoutResult(deleteData);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("name", "test_name");
		map.put("specialchars", "()*&^%$!@{}{}:\"\\\';::");
		map.put("id", 12345);
		map.put("double", 12345.333);
		map.put("data_time", "2001-01-01 01:01:01.00");
		obj.putAll(map);
		JSONArray expectedArray = new JSONArray();
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_testQueryAggregate", obj);
		JSONObject expectedObject = new JSONObject();
		expectedObject.put("$1", "1");
		expectedArray.add(expectedObject);
		JDBCTestUtils.insertData(objMap, "default");
		String query = "select count(*) from default";
		JSONArray actualArray = JDBCTestUtils.runQueryAndExtractMap(query);
		assertEquals(expectedArray, actualArray);
	 }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testQueryAggregateWithAlias() throws Exception
	{
		JDBCTestUtils.setConnection();
		JSONObject obj = new JSONObject();
		String deleteData = "delete from default";
		JDBCTestUtils.runQueryWithoutResult(deleteData);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("name", "test_name");
		map.put("specialchars", "()*&^%$!@{}{}:\"\\\';::");
		map.put("id", 12345);
		map.put("double", 12345.333);
		map.put("data_time", "2001-01-01 01:01:01.00");
		obj.putAll(map);
		JSONArray expectedArray = new JSONArray();
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_testQueryAggregateWithAlias", obj);
		JSONObject expectedObject = new JSONObject();
		expectedObject.put("alias_count", "1");
		expectedArray.add(expectedObject);
		JDBCTestUtils.insertData(objMap, "default");
		String query = "select count(*) as alias_count from default";
		JSONArray actualArray = JDBCTestUtils.runQueryAndExtractMap(query);
		assertEquals(expectedArray, actualArray);
	 }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testNestedSelectAllData() throws Exception
	{
		JDBCTestUtils.setConnection();
		JSONObject obj = new JSONObject();
		String deleteData = "delete from default";
		JDBCTestUtils.runQueryWithoutResult(deleteData);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("name", "test_name");
		map.put("id", 12345);
		JSONObject nestedObject = new JSONObject();
		nestedObject.put("nest_field_name", "test_nest");
		nestedObject.put("nest_field_id", "1");
		JSONArray expectedArray = new JSONArray();
		obj.put("nested_data", nestedObject);
		obj.putAll(map);
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_testNestedSelectAllData", obj);
		expectedArray.add(obj);
		JDBCTestUtils.insertData(objMap, "default");
		String query = "select * from default";
		JSONArray actualArray = JDBCTestUtils.runQueryAndExtractMap(query);
		assertEquals(expectedArray, actualArray);
	 }
	
	@SuppressWarnings({ "unchecked", "resource" })
	@Test
	public void testSeriesOfObjectsWithDifferentFields() throws Exception
	{
		JDBCTestUtils.setConnection();
		JSONObject obj = new JSONObject();
		String deleteData = "delete from default";
		JDBCTestUtils.runQueryWithoutResult(deleteData);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("name", "test_name");
		map.put("id", 12345);
		JSONObject nestedObject1 = new JSONObject();
		JSONObject nestedObject2  = new JSONObject();
		nestedObject1.put("nest_field_name_1", "test_nest_1");
		nestedObject1.put("nest_field_id_1", 1);
		nestedObject2.put("nest_field_name_2", "test_nest_2");
		nestedObject2.put("nest_field_id_2", 2);
		JSONArray expectedArray = new JSONArray();
		obj.put("nested_data_1", nestedObject1);
		obj.put("nested_data_2", nestedObject2);
		obj.putAll(map);
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_testSeriesOfObjectsWithDifferentFields", obj);
		JSONObject expectedJSONObject = new JSONObject();
		expectedJSONObject.put("nested_data_1", nestedObject1);
		expectedJSONObject.put("nested_data_2", nestedObject2);
		expectedArray.add(expectedJSONObject);
		JDBCTestUtils.insertData(objMap, "default");
		String query = "select default.nested_data_2.* , default.nested_data_1.*  from default";
		JDBCTestUtils.setConnection();
 
        try ( Connection con = JDBCTestUtils.con)
        {
            try (Statement stmt = con.createStatement())
            {
                try (ResultSet rs = stmt.executeQuery(query))
                {
                	while(rs.next()){
                		CBResultSet cbrs = (CBResultSet) rs;
                		java.sql.ResultSetMetaData meta = cbrs.getMetaData();
                		System.out.println(" number of columns "+meta.getColumnCount());
		                SQLJSON jsonVal1 = cbrs.getSQLJSON("nest_field_id_2");
		                assertEquals(2, jsonVal1.getInt());
		                jsonVal1 = cbrs.getSQLJSON("nest_field_id_1");
		                assertEquals(1, jsonVal1.getInt());
		                jsonVal1 = cbrs.getSQLJSON("nest_field_name_1");
		                assertEquals("test_nest_1", jsonVal1.getString());
		                jsonVal1 = cbrs.getSQLJSON("nest_field_name_2");
		                assertEquals("test_nest_2", jsonVal1.getString());
                	}
                }
            }
        }
	 }
	
	@SuppressWarnings({ "unchecked", "resource" })
	@Test
	public void testSeriesOfObjectsWithSimilarFields() throws Exception
	{
		JDBCTestUtils.setConnection();
		JSONObject obj = new JSONObject();
		String deleteData = "delete from default";
		JDBCTestUtils.runQueryWithoutResult(deleteData);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("name", "test_name");
		map.put("id", 12345);
		JSONObject nestedObject1 = new JSONObject();
		JSONObject nestedObject2  = new JSONObject();
		nestedObject1.put("nest_field_name_1", "test_nest_1");
		nestedObject1.put("nest_field_id_1", 1);
		nestedObject2.put("nest_field_name_1", "test_nest_2");
		nestedObject2.put("nest_field_id_1", 2);
		JSONArray expectedArray = new JSONArray();
		obj.put("nested_data_1", nestedObject1);
		obj.put("nested_data_2", nestedObject2);
		obj.putAll(map);
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_testSeriesOfObjectsWithSimilarFields", obj);
		JSONObject expectedJSONObject = new JSONObject();
		expectedJSONObject.put("nested_data_1", nestedObject1);
		expectedJSONObject.put("nested_data_2", nestedObject2);
		expectedArray.add(expectedJSONObject);
		JDBCTestUtils.insertData(objMap, "default");
		String query = "select default.nested_data_2.* , default.nested_data_1.*  from default";
		JDBCTestUtils.setConnection();
 
        try ( Connection con = JDBCTestUtils.con)
        {
            try (Statement stmt = con.createStatement())
            {
                try (ResultSet rs = stmt.executeQuery(query))
                {
                	while(rs.next()){
                		CBResultSet cbrs = (CBResultSet) rs;
                		java.sql.ResultSetMetaData meta = cbrs.getMetaData();
                		System.out.println(" number of columns "+meta.getColumnCount());
		                SQLJSON jsonVal1 = cbrs.getSQLJSON(1);
		                assertEquals(1, jsonVal1.getInt());
		                jsonVal1 = cbrs.getSQLJSON(2);
		                assertEquals(2, jsonVal1.getInt());
		                jsonVal1 = cbrs.getSQLJSON(3);
		                assertEquals("test_nest_1", jsonVal1.getString());
		                jsonVal1 = cbrs.getSQLJSON(4);
		                assertEquals("test_nest_2", jsonVal1.getString());
                	}
                }
            }
        }
	 }
	
	
	@SuppressWarnings("unchecked")
	@Test
	public void testArrayAndNestedSelectAllData() throws Exception
	{
		JDBCTestUtils.setConnection();
		JSONObject obj = new JSONObject();
		String deleteData = "delete from default";
		JDBCTestUtils.runQueryWithoutResult(deleteData);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("name", "test_name");
		map.put("id", 12345);
		JSONObject nestedObject = new JSONObject();
		nestedObject.put("nest_field_name", "test_nest");
		nestedObject.put("nest_field_id", 1);
		JSONArray jsonarrayForNesting = new JSONArray();
		for(int i=0;i<1000;++i){
			jsonarrayForNesting.add(i);
		}
		nestedObject.put("nested_array", jsonarrayForNesting);
		JSONArray expectedArray = new JSONArray();
		obj.put("nested_data", nestedObject);
		obj.putAll(map);
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_testArrayAndNestedSelectAllData", obj);
		expectedArray.add(obj);
		JDBCTestUtils.insertData(objMap, "default");
		String query = "select * from default";
		JSONArray actualArray = JDBCTestUtils.runQueryAndExtractMap(query);
		assertEquals(expectedArray, actualArray);
	 }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testFieldAlias() throws Exception
	{
		JDBCTestUtils.setConnection();
		JSONObject obj = new JSONObject();
		String deleteData = "delete from default";
		JDBCTestUtils.runQueryWithoutResult(deleteData);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("name", "test_name");
		map.put("id", 12345);
		map.put("double", 12345.333);
		map.put("data_time", "2001-01-01 01:01:01.00");
		obj.putAll(map);
		JSONArray expectedArray = new JSONArray();
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_testFieldAlias", obj);
		JSONObject jObject = new JSONObject();
		jObject.put("alias_name", "test_name");
		jObject.put("alias_id", 12345);
		expectedArray.add(jObject);
		JDBCTestUtils.insertData(objMap, "default");
		String query = "select name as alias_name, id as alias_id from default";
		JSONArray actualArray = JDBCTestUtils.runQueryAndExtractMap(query);
		assertEquals(expectedArray, actualArray);
	 }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testArray() throws Exception
	{
		JDBCTestUtils.setConnection();
		JSONObject obj = new JSONObject();
		String deleteData = "delete from default";
		JDBCTestUtils.runQueryWithoutResult(deleteData);
		HashMap<String, Object> map = new HashMap<String, Object>();
		JSONArray jsonarray = new JSONArray();
		for(int i=0;i<100;++i){
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("id", i) ;
			jsonObject.put("value", i) ;
			jsonarray.add(jsonObject);
		}
		map.put("name", "testArray" );
		map.put("json_array", jsonarray);
		obj.putAll(map);
		JSONArray expectedArray = new JSONArray();
		expectedArray.add(obj);
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_testArray", obj);;
		JDBCTestUtils.insertData(objMap, "default");
		String query = "select * from default";
		JSONArray actualArray = JDBCTestUtils.runQueryAndExtractMap(query);
		assertEquals(expectedArray.toJSONString(), actualArray.toJSONString());
	 }

	@SuppressWarnings("unchecked")
	@Test
	public void testOneFieldCall() throws Exception
	{
		JDBCTestUtils.setConnection();
		JSONArray array = new JSONArray();
		JSONObject obj = new JSONObject();
		String deleteData = "delete from default";
		JDBCTestUtils.runQueryWithoutResult(deleteData);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("name", "test_name");
		obj.putAll(map);
		array.add(obj);
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_unchecked", obj);
		JDBCTestUtils.insertData(objMap, "default");
		String query = "select name from default";
		JSONArray array1 = JDBCTestUtils.runQueryAndExtractMap(query);
		assertEquals(array,array1);
	 } 

}
