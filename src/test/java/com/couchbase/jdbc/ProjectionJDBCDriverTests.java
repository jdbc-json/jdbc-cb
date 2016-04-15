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
public class ProjectionJDBCDriverTests {
	
	@BeforeClass
	public static void init() throws Exception
	{
		TestUtil.resetEnvironmentProperties(null);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testSimpleData() throws Exception
	{ 
		JDBCTestUtils.setConnection(null);
		JSONObject obj = new JSONObject();
		JSONObject jsonObjNew = new JSONObject();
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("name", "test_name");
		map.put("specialchars", "()*&^%$!@{}{}:\"\\\';::");
		map.put("id", 12345);
		map.put("double", 12345.333);
		map.put("boolean", true);
		map.put("data_time", "2001-01-01 01:01:01.00");
		map.put("testid", "ProjectionJDBCDriverTests.testSimpleData");
		obj.putAll(map);
		JSONArray expectedArray = new JSONArray();
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_testSimpleData", obj);
		expectedArray.add(obj);
		JDBCTestUtils.insertData(objMap, "default");
		Thread.sleep(5000);
		String query = "select * from default where testid = 'ProjectionJDBCDriverTests.testSimpleData'";
		JDBCTestUtils.resetConnection();
		try ( Connection con = JDBCTestUtils.con)
        {
            try (Statement stmt = con.createStatement())
            {
                try (ResultSet rs = stmt.executeQuery(query))
                {
                	while(rs.next()){
                		CBResultSet cbrs = (CBResultSet) rs;
                		java.sql.ResultSetMetaData meta = cbrs.getMetaData();
		                SQLJSON jsonVal1 = cbrs.getSQLJSON("default");
		                Map actualMap = jsonVal1.getMap();
                		if(actualMap != null){
                			jsonObjNew.putAll(actualMap);
                		}
		                assertEquals(obj, jsonObjNew);
                	}
                }
                finally{
                	stmt.executeUpdate("delete from default");
                	Thread.sleep(10000);
                }
            }
            
        }
	 }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testNulls() throws Exception
	{
		JDBCTestUtils.setConnection(null);
		JDBCTestUtils.deleteDataFromBucket("default");
		JSONObject obj = new JSONObject();
		JSONObject jsonObjNew = new JSONObject();
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("name", null);
		obj.putAll(map);
		JSONArray expectedArray = new JSONArray();
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_testNulls", obj);
		expectedArray.add(obj);
		JDBCTestUtils.insertData(objMap, "default");
		Thread.sleep(5000);
		String query = "select * from default";
		JDBCTestUtils.resetConnection();
		try ( Connection con = JDBCTestUtils.con)
        {
            try (Statement stmt = con.createStatement())
            {
                try (ResultSet rs = stmt.executeQuery(query))
                {
                	while(rs.next()){
                		CBResultSet cbrs = (CBResultSet) rs;
                		java.sql.ResultSetMetaData meta = cbrs.getMetaData();
		                SQLJSON jsonVal1 = cbrs.getSQLJSON("default");
		                Map actualMap = jsonVal1.getMap();
                		if(actualMap != null){
                			jsonObjNew.putAll(actualMap);
                		}
		                assertEquals(obj, jsonObjNew);
                	}
                }
                finally{
                	stmt.executeUpdate("delete from default");
                	Thread.sleep(10000);
                }
            }
            
        }
	 }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testSimpleDataWithNull() throws Exception
	{
		JDBCTestUtils.setConnection(null);
		JDBCTestUtils.deleteDataFromBucket("default");
		JSONObject obj = new JSONObject();
		JSONObject jsonObjNew = new JSONObject();
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("name", null);
		obj.putAll(map);
		JSONArray expectedArray = new JSONArray();
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_testSimpleDataWithNull", obj);
		expectedArray.add(obj);
		JDBCTestUtils.insertData(objMap, "default");
		Thread.sleep(5000);
		String query = "select * from default";
		JDBCTestUtils.resetConnection();
		try ( Connection con = JDBCTestUtils.con)
        {
            try (Statement stmt = con.createStatement())
            {
                try (ResultSet rs = stmt.executeQuery(query))
                {
                	while(rs.next()){
                		CBResultSet cbrs = (CBResultSet) rs;
                		java.sql.ResultSetMetaData meta = cbrs.getMetaData();
		                SQLJSON jsonVal1 = cbrs.getSQLJSON("default");
		                Map actualMap = jsonVal1.getMap();
                		if(actualMap != null){
                			jsonObjNew.putAll(actualMap);
                		}
		                assertEquals(obj, jsonObjNew);
                	}
                }
                finally{
                	stmt.executeUpdate("delete from default");
                	Thread.sleep(10000);
                }

            }
            
        }
	 }
	
	@SuppressWarnings({ "unchecked", "deprecation", "resource" })
	@Test
	public void testSimpleDataDifferentDataTypesAsFields() throws Exception
	{
		JDBCTestUtils.setConnection(null);
		JSONObject obj = new JSONObject();
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
		Thread.sleep(20000);
		String query = "select name,specialchars,int,double,data_time from default";
		JDBCTestUtils.setConnection(null);
        try ( Connection con = JDBCTestUtils.con)
        {
            try (Statement stmt = con.createStatement())
            {
                try (ResultSet rs = stmt.executeQuery(query))
                {
                	while(rs.next()){
                		CBResultSet cbrs = (CBResultSet) rs;
                		java.sql.ResultSetMetaData meta = cbrs.getMetaData();
		                SQLJSON jsonVal1 = cbrs.getSQLJSON("name");
		                assertEquals(12, jsonVal1.getJDBCType());
		                assertEquals("test_name", jsonVal1.getString());
		                jsonVal1 = cbrs.getSQLJSON("specialchars");
		                assertEquals(12, jsonVal1.getJDBCType());
		                assertEquals("()*&^%$!@{}{}:\"\\\';::", jsonVal1.getString());
		                jsonVal1 = cbrs.getSQLJSON("int");
		                assertEquals(2, jsonVal1.getJDBCType());
		                assertEquals(12345, jsonVal1.getInt());
		                jsonVal1 = cbrs.getSQLJSON("double");
		                assertEquals(2, jsonVal1.getJDBCType());
		                //assertEquals(12345.333, jsonVal1.getDouble());
                	}
                }
                finally{
                	stmt.executeUpdate("delete from default");
                	Thread.sleep(10000);
                }
            }
            
        }
	 }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testNestedOperator() throws Exception
	{
		JDBCTestUtils.setConnection(null);
		JSONObject obj = new JSONObject();
		JSONObject nestedobj = new JSONObject();
		JSONObject jsonObjNew = new JSONObject();
		HashMap<String, Object> map = new HashMap<String, Object>();
		nestedobj.put("nested.nested", "1");
		map.put("name", "test_name");
		map.put("specialchars", "()*&^%$!@{}{}:\"\\\';::");
		map.put("id", 12345);
		map.put("double", 12345.333);
		map.put("data_time", "2001-01-01 01:01:01.00");
		obj.put("nested", nestedobj);
		obj.putAll(map);
		JSONArray expectedArray = new JSONArray();
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_testNestedOperator", obj);
		expectedArray.add(nestedobj);
		JDBCTestUtils.insertData(objMap, "default");
		Thread.sleep(10000);
		String query = "select default.nested.* from default";
		JDBCTestUtils.resetConnection();
		try ( Connection con = JDBCTestUtils.con)
        {
            try (Statement stmt = con.createStatement())
            {
                try (ResultSet rs = stmt.executeQuery(query))
                {
                	while(rs.next()){
		                assertEquals("1", rs.getString("nested.nested"));
                	}
                }
                finally{
                	stmt.executeUpdate("delete from default");
                	Thread.sleep(10000);
                }
            }
            
        }
	 }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testNoResult() throws Exception
	{
		JDBCTestUtils.setConnection(null);
		JSONObject obj = new JSONObject();
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("name", "test_name");
		obj.putAll(map);
		JSONArray expectedArray = new JSONArray();
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("2_testNestedOperator", obj);
		JDBCTestUtils.insertData(objMap, "default");
		Thread.sleep(10000);
		String query = "select * from default where x=1234";
		JSONArray actualArray = JDBCTestUtils.runQueryAndExtractMap(query);
		assertEquals(expectedArray, actualArray);

		// Clean up after ourselves.
        JDBCTestUtils.setConnection(null);
		Statement stmt = JDBCTestUtils.con.createStatement();
		stmt.executeUpdate("delete from default");
        Thread.sleep(10000);
	 }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testNoResultWithBucketEmpty() throws Exception
	{
		JDBCTestUtils.setConnection(null);
		JSONArray expectedArray = new JSONArray();
		String query = "select * from default where x=1234";
		JSONArray actualArray = JDBCTestUtils.runQueryAndExtractMap(query);
		assertEquals(expectedArray, actualArray);
	 }
	
	@SuppressWarnings("unchecked")
	//@Test
	public void testQueryAggregate() throws Exception
	{
		JDBCTestUtils.setConnection(null);
		JSONObject obj = new JSONObject();
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
		JDBCTestUtils.setConnection(null);
		JSONObject obj = new JSONObject();
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
		expectedObject.put("name", "test_value");
		expectedArray.add(expectedObject);
		JDBCTestUtils.insertData(objMap, "default");
		Thread.sleep(5000);
		String query = "select count(*) as alias_count from default";
		JDBCTestUtils.resetConnection();
		try ( Connection con = JDBCTestUtils.con)
        {
            try (Statement stmt = con.createStatement())
            {
                try (ResultSet rs = stmt.executeQuery(query))
                {
                	while(rs.next()){
                		CBResultSet cbrs = (CBResultSet) rs;
                		java.sql.ResultSetMetaData meta = cbrs.getMetaData();
		                SQLJSON jsonVal1 = cbrs.getSQLJSON("alias_count");
		                int actual = jsonVal1.getInt();
		                assertEquals(1, actual);
                	}
                }
                finally{
                	stmt.executeUpdate("delete from default");
                	Thread.sleep(10000);
                }
            } 
        }
	 }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testNestedSelectAllData() throws Exception
	{
		JDBCTestUtils.setConnection(null);
		JSONObject obj = new JSONObject();
		JSONObject jsonObjNew = new JSONObject();
		
		HashMap<String, Object> map = new HashMap<String, Object>();
		JSONObject nestedObject = new JSONObject();
		obj.put("nested_data", nestedObject);
		obj.putAll(map);
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_testNestedSelectAllData", obj);
		JDBCTestUtils.insertData(objMap, "default");
		Thread.sleep(15000);
		String query = "select default.* from default";
		JDBCTestUtils.resetConnection();
		try ( Connection con = JDBCTestUtils.con)
        {
            try (Statement stmt = con.createStatement())
            {
                try (ResultSet rs = stmt.executeQuery(query))
                {
                	while(rs.next()){
		                assertEquals("{}", rs.getString("nested_data"));
                	}
                }
                finally{
                	stmt.executeUpdate("delete from default");
                	Thread.sleep(10000);
                }
            } 
        }
	 }
	
	@SuppressWarnings({ "unchecked", "resource" })
	@Test
	public void testSeriesOfObjectsWithSimilarFields() throws Exception
	{
		JDBCTestUtils.setConnection(null);
		JSONObject obj = new JSONObject();
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
		objMap.put("1_testSeriesOfObjectsWithSimilarFields", obj);
		JSONObject expectedJSONObject = new JSONObject();
		expectedJSONObject.put("nested_data_1", nestedObject1);
		expectedJSONObject.put("nested_data_2", nestedObject2);
		expectedArray.add(expectedJSONObject);
		JDBCTestUtils.insertData(objMap, "default");
		Thread.sleep(20000);
		String query = "select default.nested_data_2.* , default.nested_data_1.*  from default";
		JDBCTestUtils.setConnection(null);
 
        try ( Connection con = JDBCTestUtils.con)
        {
            try (Statement stmt = con.createStatement())
            {
                try (ResultSet rs = stmt.executeQuery(query))
                {
                	while(rs.next()){
		                assertEquals(1, rs.getInt("nest_field_id_1"));
		                assertEquals("2", rs.getString("nest_field_id_2"));
		                assertEquals("test_nest_1", rs.getString("nest_field_name_1"));
		                assertEquals("test_nest_2", rs.getString("nest_field_name_2"));
                	}
                }
                finally{
                	stmt.executeUpdate("delete from default");
                	Thread.sleep(10000);
                }
            }
        }
	 }
	
	
	@SuppressWarnings("unchecked")
	@Test
	public void testArrayAndNestedSelectAllData() throws Exception
	{
		JDBCTestUtils.setConnection(null);
		JSONObject obj = new JSONObject();
		JSONObject jsonObjNew = new JSONObject();
		HashMap<String, Object> map = new HashMap<String, Object>();
		JSONObject nestedObject = new JSONObject();
		JSONArray jsonarrayForNesting = new JSONArray();
		for(int i=0;i<100;++i){
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
		Thread.sleep(30000);
		String query = "select nested_data.* from default";
		JDBCTestUtils.resetConnection();
		try ( Connection con = JDBCTestUtils.con)
        {
            try (Statement stmt = con.createStatement())
            {
                try (ResultSet rs = stmt.executeQuery(query))
                {
                	while(rs.next()){
                		CBResultSet cbrs = (CBResultSet) rs;
                		java.sql.ResultSetMetaData meta = cbrs.getMetaData();
		                SQLJSON jsonVal1 = cbrs.getSQLJSON("nested_array");
		                Object obj1 = jsonVal1.getObject();
		                assertEquals(jsonarrayForNesting.toString().replace(" ",""), obj1.toString().replace(" ",""));
                	}
                }
                finally{
                	stmt.executeUpdate("delete from default");
                	Thread.sleep(10000);
                }
            } 
        }
	 }
	
	@SuppressWarnings("unchecked")
	@Test
	public void testFieldAlias() throws Exception
	{
		JDBCTestUtils.setConnection(null);
		JSONObject obj = new JSONObject();
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
		Thread.sleep(5000);
		String query = "select name as alias_name, id as alias_id from default";
		JDBCTestUtils.resetConnection();
		try ( Connection con = JDBCTestUtils.con)
        {
            try (Statement stmt = con.createStatement())
            {
                try (ResultSet rs = stmt.executeQuery(query))
                {
                	while(rs.next()){
                		CBResultSet cbrs = (CBResultSet) rs;
                		java.sql.ResultSetMetaData meta = cbrs.getMetaData();
		                SQLJSON jsonVal1 = cbrs.getSQLJSON("alias_name");
		                String actualString = jsonVal1.getString();
		                assertEquals("test_name", actualString);
                	}
                
		}
                finally{
                	stmt.executeUpdate("delete from default");
                	Thread.sleep(10000);
                }
            }
            
        }
	 }
	
	
	@SuppressWarnings("unchecked")
	@Test
	public void testOneFieldCall() throws Exception
	{
		JDBCTestUtils.setConnection(null);
		JSONObject obj = new JSONObject();
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("name", "test_name");
		obj.putAll(map);
		HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
		objMap.put("1_unchecked", obj);
		Thread.sleep(2000);
		JDBCTestUtils.insertData(objMap, "default");
		String query = "select name from default";
		JDBCTestUtils.resetConnection();
		try ( Connection con = JDBCTestUtils.con)
        {
            try (Statement stmt = con.createStatement())
            {
                try (ResultSet rs = stmt.executeQuery(query))
                {
                	while(rs.next()){
                		CBResultSet cbrs = (CBResultSet) rs;
                		java.sql.ResultSetMetaData meta = cbrs.getMetaData();
		                SQLJSON jsonVal1 = cbrs.getSQLJSON("name");
		                String actualString = jsonVal1.getString();
		                assertEquals("test_name", actualString);
                	}
                }
                finally{
                	stmt.executeUpdate("delete from default");
                	Thread.sleep(10000);
                }
            }
			
            
        }
	 } 

}
