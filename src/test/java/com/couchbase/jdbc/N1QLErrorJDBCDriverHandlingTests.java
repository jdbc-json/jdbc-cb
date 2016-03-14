package com.couchbase.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.couchbase.jdbc.CBResultSet;
import com.couchbase.json.SQLJSON;

@RunWith(JUnit4.class)
public class N1QLErrorJDBCDriverHandlingTests {
	@BeforeClass
	public static void initializeCluster() throws Exception
	{
		TestUtil.resetEnvironmentProperties(null);
		TestUtil.initializeCluster(false);
	}
	
	@AfterClass
	public static void cleanupCluster() throws Exception
	{
		TestUtil.destroyCluster();
	}
	
	@Test
	public void testQueryMissingBucket(){
	String query = "select * from does_not_exist_bucket";
	JDBCTestUtils.setConnection(null);
	        try ( Connection con = JDBCTestUtils.con)
	        {
	            try (Statement stmt = con.createStatement())
	            {
	                stmt.executeQuery(query);
	            }
	        } catch (SQLException e) {
	assertEquals("Keyspace not found keyspace does_not_exist_bucket - cause: No bucket named does_not_exist_bucket",e.getMessage());
	}
	}
	
	@Test
	public void testQueryWithoutAnyDependentIndex(){
	String query = "select * from default";
	JDBCTestUtils.setConnection(null);
	        try ( Connection con = JDBCTestUtils.con)
	        {
	            try (Statement stmt = con.createStatement())
	            {
	                stmt.executeQuery(query);
	            }
	        } catch (SQLException e) {
	assertEquals("No primary index on keyspace default. Use CREATE PRIMARY INDEX to create one.",e.getMessage());
	}
	}
	
	@Test
	public void testQueryWithQueryServerDown(){
	try{
	    
	DriverManager.getConnection("jdbc:couchbase://0.0.0.1:9499") ;
	    
	}catch(SQLException ex){
	    
	assertEquals("Error opening connection",ex.getMessage());
	    
	} 
	}
	
	@Test
	public void testQueryWithIncorrectSQLSyntax(){
	try {
	Thread.sleep(1000);
	} catch (InterruptedException e1) {
	e1.printStackTrace();
	}
	JDBCTestUtils.createPrimaryIndexes(TestUtil.clusterInfo.bucketInformation.keySet());
	String query = "crap this is wrong syntax";
	JDBCTestUtils.setConnection(null);
	        try ( Connection con = JDBCTestUtils.con)
	        {
	            try (Statement stmt = con.createStatement())
	            {
	                stmt.executeQuery(query);
	            }
	        } catch (SQLException e) {
	assertEquals("syntax error - at this",e.getMessage());
	}
	        String drop_primary_index = "drop primary index on default";
	        try {
	JDBCTestUtils.runQueryWithoutResult(drop_primary_index);
	} catch (SQLException e) {
	e.printStackTrace();
	}  
	}
	
	@SuppressWarnings({ "unchecked", "resource" })
	@Test
	public void testNumberNotPresent() throws Exception
	{
	JDBCTestUtils.setConnection(null);
	String drop_primary_index = "drop primary index on default";
	JDBCTestUtils.createPrimaryIndexes(TestUtil.clusterInfo.bucketInformation.keySet());
	JSONObject obj = new JSONObject();
	String deleteData = "delete from default";
	JDBCTestUtils.runQueryWithoutResult(deleteData);
	HashMap<String, Object> map = new HashMap<String, Object>();
	map.put("name", "xx");
	obj.putAll(map);
	JSONArray expectedArray = new JSONArray();
	HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
	objMap.put("1", obj);
	expectedArray.add(obj);
	JDBCTestUtils.insertData(objMap, "default");
	Thread.sleep(1000);
	String query = "select name from default";
	JDBCTestUtils.setConnection(null);
	            try (Statement stmt = JDBCTestUtils.con.createStatement())
	            {
	                try (ResultSet rs = stmt.executeQuery(query))
	                {
	                
	CBResultSet cbrs = (CBResultSet) rs;
	                
	while(cbrs.next()){
	                
	java.sql.ResultSetMetaData meta = cbrs.getMetaData();
	                SQLJSON jsonVal = cbrs.getSQLJSON(1);
	                try{
	                jsonVal.getInt();
	                }catch(SQLException e){
	                String expectatedMessage = "value xx not a number";
	                assertEquals(expectatedMessage.trim(), e.getMessage().trim());
	                }
	                try{
	                jsonVal.getDouble();
	                }catch(SQLException e){
	                String expectatedMessage = "value xx not a number";
	                assertEquals(expectatedMessage.trim(), e.getMessage().trim());
	                }
	                try{
	                jsonVal.getLong();
	                }catch(SQLException e){
	                String expectatedMessage = "value xx not a number";
	                assertEquals(expectatedMessage.trim(), e.getMessage().trim());
	                }
	                try{
	                jsonVal.getShort();
	                }catch(SQLException e){
	                String expectatedMessage = "value xx not a number";
	                assertEquals(expectatedMessage.trim(), e.getMessage().trim());
	                }
	                try{
	                jsonVal.getFloat();
	                }catch(SQLException e){
	                String expectatedMessage = "value xx not a number";
	                assertEquals(expectatedMessage.trim(), e.getMessage().trim());
	                }
	                
	}
	                }
	            }
	
	try {
	JDBCTestUtils.runQueryWithoutResult(drop_primary_index);
	} catch (SQLException e) {
	e.printStackTrace();
	} 
	}
	
	@SuppressWarnings({ "unchecked", "resource" })
	@Test
	public void testStringNotPresent() throws Exception
	{
	JDBCTestUtils.setConnection(null);
	String drop_primary_index = "drop primary index on default";
	JDBCTestUtils.createPrimaryIndexes(TestUtil.clusterInfo.bucketInformation.keySet());
	JSONObject obj = new JSONObject();
	String deleteData = "delete from default";
	JDBCTestUtils.runQueryWithoutResult(deleteData);
	HashMap<String, Object> map = new HashMap<String, Object>();
	map.put("num_1", 1);
	obj.putAll(map);
	JSONArray expectedArray = new JSONArray();
	HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
	objMap.put("1", obj);
	expectedArray.add(obj);
	JDBCTestUtils.insertData(objMap, "default");
	Thread.sleep(1000);
	String query = "select num_1 from default";
	JDBCTestUtils.setConnection(null);
	            try (Statement stmt = JDBCTestUtils.con.createStatement())
	            {
	                try (ResultSet rs = stmt.executeQuery(query))
	                {
	                
	CBResultSet cbrs = (CBResultSet) rs;
	                
	while(cbrs.next()){
	                
	java.sql.ResultSetMetaData meta = cbrs.getMetaData();
	                SQLJSON jsonVal = cbrs.getSQLJSON(1);
	                try{
	                jsonVal.getString();
	                }catch(SQLException e){
	                String expectatedMessage = "value 1 not a string";
	                assertEquals(expectatedMessage.trim(), e.getMessage().trim());
	                }
	               
	                
	}
	                }
	            }
	
	try {
	JDBCTestUtils.runQueryWithoutResult(drop_primary_index);
	} catch (SQLException e) {
	e.printStackTrace();
	} 
	}
	
	@SuppressWarnings({ "unchecked", "resource" })
	@Test
	public void testMapNotPresent() throws Exception
	{
	JDBCTestUtils.setConnection(null);
	String drop_primary_index = "drop primary index on default";
	JDBCTestUtils.createPrimaryIndexes(TestUtil.clusterInfo.bucketInformation.keySet());
	JSONObject obj = new JSONObject();
	String deleteData = "delete from default";
	JDBCTestUtils.runQueryWithoutResult(deleteData);
	HashMap<String, Object> map = new HashMap<String, Object>();
	map.put("name", "crap");
	obj.putAll(map);
	JSONArray expectedArray = new JSONArray();
	HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
	objMap.put("1", obj);
	expectedArray.add(obj);
	JDBCTestUtils.insertData(objMap, "default");
	Thread.sleep(1000);
	String query = "select name from default";
	JDBCTestUtils.setConnection(null);
	            try (Statement stmt = JDBCTestUtils.con.createStatement())
	            {
	                try (ResultSet rs = stmt.executeQuery(query))
	                {
	                
	CBResultSet cbrs = (CBResultSet) rs;
	                
	while(cbrs.next()){
	                
	java.sql.ResultSetMetaData meta = cbrs.getMetaData();
	                SQLJSON jsonVal = cbrs.getSQLJSON(1);
	                try{
	                jsonVal.getMap();
	                }catch(SQLException e){
	                String expectatedMessage = "Value crap is not a map";
	                assertEquals(expectatedMessage.trim(), e.getMessage().trim());
	                }
	               
	                
	}
	                }
	            }
	
	try {
	JDBCTestUtils.runQueryWithoutResult(drop_primary_index);
	} catch (SQLException e) {
	e.printStackTrace();
	} 
	}
	
	@SuppressWarnings({ "unchecked", "resource" })
	@Test
	public void testArrayNotPresent() throws Exception
	{
	JDBCTestUtils.setConnection(null);
	String drop_primary_index = "drop primary index on default";
	JDBCTestUtils.createPrimaryIndexes(TestUtil.clusterInfo.bucketInformation.keySet());
	JSONObject obj = new JSONObject();
	String deleteData = "delete from default";
	JDBCTestUtils.runQueryWithoutResult(deleteData);
	HashMap<String, Object> map = new HashMap<String, Object>();
	map.put("name", "crap");
	obj.putAll(map);
	JSONArray expectedArray = new JSONArray();
	HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
	objMap.put("1", obj);
	expectedArray.add(obj);
	JDBCTestUtils.insertData(objMap, "default");
	Thread.sleep(1000);
	String query = "select name from default";
	JDBCTestUtils.setConnection(null);
	            try (Statement stmt = JDBCTestUtils.con.createStatement())
	            {
	                try (ResultSet rs = stmt.executeQuery(query))
	                {
	                
	CBResultSet cbrs = (CBResultSet) rs;
	                
	while(cbrs.next()){
	                
	java.sql.ResultSetMetaData meta = cbrs.getMetaData();
	                SQLJSON jsonVal = cbrs.getSQLJSON(1);
	                try{
	                jsonVal.getArray();
	                }catch(SQLException e){
	                String expectatedMessage = "value crap not a array";
	                assertEquals(expectatedMessage.trim(), e.getMessage().trim());
	                }
	               
	                
	}
	                }
	            }
	
	try {
	JDBCTestUtils.runQueryWithoutResult(drop_primary_index);
	} catch (SQLException e) {
	e.printStackTrace();
	} 
	}
	
	@SuppressWarnings({ "unchecked", "resource" })
	@Test
	public void testDateNotPresent() throws Exception
	{
	JDBCTestUtils.setConnection(null);
	String drop_primary_index = "drop primary index on default";
	JDBCTestUtils.createPrimaryIndexes(TestUtil.clusterInfo.bucketInformation.keySet());
	JSONObject obj = new JSONObject();
	String deleteData = "delete from default";
	JDBCTestUtils.runQueryWithoutResult(deleteData);
	HashMap<String, Object> map = new HashMap<String, Object>();
	map.put("name", "NAME");
	obj.putAll(map);
	JSONArray expectedArray = new JSONArray();
	HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
	objMap.put("1", obj);
	expectedArray.add(obj);
	JDBCTestUtils.insertData(objMap, "default");
	Thread.sleep(1000);
	String query = "select name from default";
	JDBCTestUtils.setConnection(null);
	            try (Statement stmt = JDBCTestUtils.con.createStatement())
	            {
	                try (ResultSet rs = stmt.executeQuery(query))
	                {
	                
	CBResultSet cbrs = (CBResultSet) rs;
	                
	while(cbrs.next()){
	                
	java.sql.ResultSetMetaData meta = cbrs.getMetaData();
	                SQLJSON jsonVal = cbrs.getSQLJSON(1);
	                try{
	                jsonVal.getDate(null);
	                }catch(SQLException e){
	                String expectatedMessage = "value NAME is not a date";
	                assertEquals(expectatedMessage.trim(), e.getMessage().trim());
	                }
	               
	                
	}
	                }
	            }
	
	try {
	JDBCTestUtils.runQueryWithoutResult(drop_primary_index);
	} catch (SQLException e) {
	e.printStackTrace();
	} 
	}
	
	@SuppressWarnings({ "unchecked", "resource" })
	@Test
	public void testTimeNotPresent() throws Exception
	{
	JDBCTestUtils.setConnection(null);
	String drop_primary_index = "drop primary index on default";
	JDBCTestUtils.createPrimaryIndexes(TestUtil.clusterInfo.bucketInformation.keySet());
	JSONObject obj = new JSONObject();
	String deleteData = "delete from default";
	JDBCTestUtils.runQueryWithoutResult(deleteData);
	HashMap<String, Object> map = new HashMap<String, Object>();
	map.put("name", "NAME");
	obj.putAll(map);
	JSONArray expectedArray = new JSONArray();
	HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
	objMap.put("1", obj);
	expectedArray.add(obj);
	JDBCTestUtils.insertData(objMap, "default");
	Thread.sleep(1000);
	String query = "select name from default";
	JDBCTestUtils.setConnection(null);
	            try (Statement stmt = JDBCTestUtils.con.createStatement())
	            {
	                try (ResultSet rs = stmt.executeQuery(query))
	                {
	                
	CBResultSet cbrs = (CBResultSet) rs;
	                
	while(cbrs.next()){
	                
	java.sql.ResultSetMetaData meta = cbrs.getMetaData();
	                SQLJSON jsonVal = cbrs.getSQLJSON(1);
	                try{
	                jsonVal.getTime(null); 
	                }catch(SQLException e){
	                String expectatedMessage = "value NAME is not a Time";
	                assertEquals(expectatedMessage.trim(), e.getMessage().trim());
	                }
	               
	                
	}
	                }
	            }
	
	try {
	JDBCTestUtils.runQueryWithoutResult(drop_primary_index);
	} catch (SQLException e) {
	e.printStackTrace();
	} 
	}
	
	@SuppressWarnings({ "unchecked", "resource" })
	@Test
	public void testTimeStampNotPresent() throws Exception
	{
	JDBCTestUtils.setConnection(null);
	String drop_primary_index = "drop primary index on default";
	JDBCTestUtils.createPrimaryIndexes(TestUtil.clusterInfo.bucketInformation.keySet());
	JSONObject obj = new JSONObject();
	String deleteData = "delete from default";
	JDBCTestUtils.runQueryWithoutResult(deleteData);
	HashMap<String, Object> map = new HashMap<String, Object>();
	map.put("name", "NAME");
	obj.putAll(map);
	JSONArray expectedArray = new JSONArray();
	HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
	objMap.put("1", obj);
	expectedArray.add(obj);
	JDBCTestUtils.insertData(objMap, "default");
	Thread.sleep(1000);
	String query = "select name from default";
	JDBCTestUtils.setConnection(null);
	            try (Statement stmt = JDBCTestUtils.con.createStatement())
	            {
	                try (ResultSet rs = stmt.executeQuery(query))
	                {
	                
	CBResultSet cbrs = (CBResultSet) rs;
	                
	while(cbrs.next()){
	                
	java.sql.ResultSetMetaData meta = cbrs.getMetaData();
	                SQLJSON jsonVal = cbrs.getSQLJSON(1);
	                try{
	                jsonVal.getTimestamp(null);
	                }catch(SQLException e){
	                String expectatedMessage = "value NAMEis not a Timestamp";
	                assertEquals(expectatedMessage.trim(), e.getMessage().trim());
	                }
	               
	                
	}
	                }
	            }
	
	try {
	JDBCTestUtils.runQueryWithoutResult(drop_primary_index);
	} catch (SQLException e) {
	e.printStackTrace();
	} 
	}
	
	@SuppressWarnings({ "unchecked", "resource" })
	@Test
	public void testByteNotPresent() throws Exception
	{
	JDBCTestUtils.setConnection(null);
	String drop_primary_index = "drop primary index on default";
	JDBCTestUtils.createPrimaryIndexes(TestUtil.clusterInfo.bucketInformation.keySet());
	JSONObject obj = new JSONObject();
	String deleteData = "delete from default";
	JDBCTestUtils.runQueryWithoutResult(deleteData);
	HashMap<String, Object> map = new HashMap<String, Object>();
	map.put("name", "NAME");
	obj.putAll(map);
	JSONArray expectedArray = new JSONArray();
	HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
	objMap.put("1", obj);
	expectedArray.add(obj);
	JDBCTestUtils.insertData(objMap, "default");
	Thread.sleep(1000);
	String query = "select name from default";
	JDBCTestUtils.setConnection(null);
	            try (Statement stmt = JDBCTestUtils.con.createStatement())
	            {
	                try (ResultSet rs = stmt.executeQuery(query))
	                {
	                
	CBResultSet cbrs = (CBResultSet) rs;
	                
	while(cbrs.next()){
	                
	java.sql.ResultSetMetaData meta = cbrs.getMetaData();
	                SQLJSON jsonVal = cbrs.getSQLJSON(1);
	                try{
	                jsonVal.getByte();
	                }catch(SQLException e){
	                String expectatedMessage = "value NAME not a number";
	                assertEquals(expectatedMessage.trim(), e.getMessage().trim());
	                }
	               
	                
	}
	                }
	            }
	
	try {
	JDBCTestUtils.runQueryWithoutResult(drop_primary_index);
	} catch (SQLException e) {
	e.printStackTrace();
	} 
	}
	
	@SuppressWarnings({ "unchecked", "resource" })
	@Test
	public void testBytesNotPresent() throws Exception
	{
	JDBCTestUtils.setConnection(null);
	String drop_primary_index = "drop primary index on default";
	JDBCTestUtils.createPrimaryIndexes(TestUtil.clusterInfo.bucketInformation.keySet());
	JSONObject obj = new JSONObject();
	String deleteData = "delete from default";
	JDBCTestUtils.runQueryWithoutResult(deleteData);
	HashMap<String, Object> map = new HashMap<String, Object>();
	map.put("name", "NAME");
	obj.putAll(map);
	JSONArray expectedArray = new JSONArray();
	HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
	objMap.put("1", obj);
	expectedArray.add(obj);
	JDBCTestUtils.insertData(objMap, "default");
	Thread.sleep(1000);
	String query = "select name from default";
	JDBCTestUtils.setConnection(null);
	            try (Statement stmt = JDBCTestUtils.con.createStatement())
	            {
	                try (ResultSet rs = stmt.executeQuery(query))
	                {
	                
	CBResultSet cbrs = (CBResultSet) rs;
	                
	while(cbrs.next()){
	                
	java.sql.ResultSetMetaData meta = cbrs.getMetaData();
	                SQLJSON jsonVal = cbrs.getSQLJSON(1);
	                try{
	                jsonVal.getBytes();
	                }catch(SQLException e){
	                String expectatedMessage = "value NAME not a number";
	                assertEquals(expectatedMessage.trim(), e.getMessage().trim());
	                }
	               
	                
	}
	                }
	            }
	
	try {
	JDBCTestUtils.runQueryWithoutResult(drop_primary_index);
	} catch (SQLException e) {
	e.printStackTrace();
	} 
	}

	@SuppressWarnings({ "unchecked", "resource" })
	@Test
	public void testBooleanPresent() throws Exception
	{
	JDBCTestUtils.setConnection(null);
	String drop_primary_index = "drop primary index on default";
	JDBCTestUtils.createPrimaryIndexes(TestUtil.clusterInfo.bucketInformation.keySet());
	JSONObject obj = new JSONObject();
	String deleteData = "delete from default";
	JDBCTestUtils.runQueryWithoutResult(deleteData);
	HashMap<String, Object> map = new HashMap<String, Object>();
	map.put("name", "NAME");
	obj.putAll(map);
	JSONArray expectedArray = new JSONArray();
	HashMap<String,JSONObject> objMap = new HashMap<String,JSONObject>();
	objMap.put("1", obj);
	expectedArray.add(obj);
	JDBCTestUtils.insertData(objMap, "default");
	Thread.sleep(1000);
	String query = "select name from default";
	JDBCTestUtils.setConnection(null);
	            try (Statement stmt = JDBCTestUtils.con.createStatement())
	            {
	                try (ResultSet rs = stmt.executeQuery(query))
	                {
	                
	CBResultSet cbrs = (CBResultSet) rs;
	                
	while(cbrs.next()){
	                
	java.sql.ResultSetMetaData meta = cbrs.getMetaData();
	                SQLJSON jsonVal = cbrs.getSQLJSON(1);
	                try{
	                jsonVal.getBoolean();
	                }catch(SQLException e){
	                String expectatedMessage = "value NAME not a boolean";
	                assertEquals(expectatedMessage.trim(), e.getMessage().trim());
	                }
	               
	                
	}
	                }
	            }
	
	try {
	JDBCTestUtils.runQueryWithoutResult(drop_primary_index);
	} catch (SQLException e) {
	e.printStackTrace();
	} 
	}
}
