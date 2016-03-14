package com.couchbase.jdbc;
import com.couchbase.jdbc.BucketInfo;
import com.couchbase.jdbc.ClusterInfo;
import com.couchbase.jdbc.ClusterSetupUtils;
import com.couchbase.jdbc.TestResultAnalysis;

import java.awt.List;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import com.couchbase.json.SQLJSON;

import java.sql.Types;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import java.util.Properties;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JDBCTestUtils {
	public static Connection con;
    public static String ConnectionURL = "jdbc:couchbase://127.0.0.1:8093";
    public static ClusterInfo clusteInfo = null;
    
    public static void setConnection(String url){
    	try{
    		if(url != null){
    			JDBCTestUtils.ConnectionURL = url;
    		}
    		JDBCTestUtils.con = DriverManager.getConnection(ConnectionURL) ;
    	}catch(SQLException ex){
    		System.out.println(ex.toString());
    	}
    }
    
    public static void setConnection(String url, Properties properties){
    	try{
    		if(url != null){
    			JDBCTestUtils.ConnectionURL = url;
    		}
    		JDBCTestUtils.con = DriverManager.getConnection(ConnectionURL) ;
    	}catch(SQLException ex){
    		System.out.println(ex.toString());
    	}
    }
    
    public static void closeConnection(){
    	try{
    		JDBCTestUtils.con.close();
    	}catch(SQLException ex){
    		System.out.println(ex.toString());
    	}
    }
    
    public static void resetConnection(){
    	try{
    		if(JDBCTestUtils.con != null){
	    		JDBCTestUtils.con.close();
    		}
	    		JDBCTestUtils.setConnection(null);
    	}catch(SQLException ex){
    		System.out.println(ex.toString());
    	}
    }
    
    /***
     * Run query and return list of String values
     * Extracted from the result set returned by query
     * @param query
     * @return
     * @throws SQLException
     */
    public static LinkedList<String> runQuery(String query) throws SQLException
    {	
        JDBCTestUtils.setConnection(null);
        LinkedList<String> objList = new LinkedList<String>();
        try ( Connection con = JDBCTestUtils.con)
        {

            try (Statement stmt = con.createStatement())
            {
                try (ResultSet rs = stmt.executeQuery(query))
                {
                	return JDBCTestUtils.extractDataFromQueryResult(rs);
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
        return objList;
    }
    
    /***
     * Run query and return map of JSONArray of JSONObjects
     * @param query
     * @return
     * @throws SQLException
     */
    public static JSONArray runQueryAndExtractMap(String query) throws SQLException
    {	
        JDBCTestUtils.setConnection(null);
        JSONArray objList = new JSONArray();
        try ( Connection con = JDBCTestUtils.con)
        {
            try (Statement stmt = con.createStatement())
            {
                try (ResultSet rs = stmt.executeQuery(query))
                {
                	return JDBCTestUtils.extractDataMapFromQueryResult(rs);
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
        return objList;
    }
    
    /***
     * Initialization cluster
     * 1. Initialize Cluster with services
     * 2. Add Buckets and load it with data
     * @param clusterInfo
     * @param inputPath
     * @return scenarioFilePath
     */
    public static String initializeCluster(
    		ClusterInfo clusterInfo,
    		String inputPath){
    	HashMap<String, HashMap<String, JSONObject>> dataMap = null; 
    	String scenarioFilePath= inputPath+"/input/source_input_rqg_run.txt";
        String inputFilePath= inputPath+"/db_dump/database_dump.zip";
		try {
			dataMap = JDBCTestUtils.extractDataInformationFromFile(inputFilePath);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		// Add Bucket Information
		for(String bucketName:dataMap.keySet()){
			BucketInfo bucketInfo = new BucketInfo();
			bucketInfo.name = bucketName;
			clusterInfo.addBucketInfo(bucketInfo);
		}
		// Create Buckets
		ClusterSetupUtils.createBuckets(clusterInfo);
		// Sleep before Loading data
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// Load data
		for(String bucketName:dataMap.keySet()){
			JDBCTestUtils.insertData(dataMap.get(bucketName), bucketName);
		}
		// Create Primary Index
    	System.out.println(" Creating Primary Indexes ");
        JDBCTestUtils.createPrimaryIndexes(clusterInfo.bucketInformation.keySet());
        // Initialize Variables
        JDBCTestUtils.ConnectionURL = String.format("jdbc:couchbase://%s:%d",
        		clusterInfo.masterNodeInfo.ip,clusterInfo.masterNodeInfo.n1qlPort);
        JDBCTestUtils.clusteInfo = clusterInfo;
    	return scenarioFilePath;
    }
    
    public static LinkedList<String> extractDataFromQueryResult(ResultSet rs){
    	    CBResultSet cbrs =(CBResultSet) rs;
    		LinkedList<String> list = new LinkedList<String>();
            ResultSetMetaData rsmd;
			try {
	            while (cbrs.next())
	            {
	            	rsmd = cbrs.getMetaData();
					int columnsNumber = rsmd.getColumnCount();
	                for (int i = 0; i < columnsNumber; i++)
	                {
	                	String type = rsmd.getColumnTypeName(i+1);
	                	if(type.equals("number")){
	                		String value = cbrs.getString(i + 1);
	                		if(value != null){
		                		Integer val = (int)Math.round(Double.parseDouble(value));
		                		list.add(val.toString());
	                		}
	                	}else if(type.equals("json")){
	                		SQLJSON jsonValue = cbrs.getSQLJSON(i+1);
	                		int jdbcType =jsonValue.getJDBCType();
	                		boolean flag = false;
	                		if (jdbcType == Types.JAVA_OBJECT){
		                            Map m = null;
		                            try{
		                            	list.add(jsonValue.getInt()+"");
		                            	flag = true;
	                				}catch(Exception e){
	                					e.printStackTrace();
	                				}
		                            try{
		                            	if(!flag){
			                            	list.add(((int)Math.round(jsonValue.getDouble()))+"");
			                            	flag = true;
		                            	}
	                				}catch(Exception e){
	                					e.printStackTrace();
	                				}
		                            try{
		                            	if(!flag){
			                            	m = jsonValue.getMap();
			                            	if (m != null){
					                            for(Object o:m.values()){
					                            	list.add(o.toString());
					                            }
		                            	}
			                            }
	                				}catch(Exception e){
	                					e.printStackTrace();
	                				}
		                            
	                		}else if(jdbcType == Types.INTEGER){
	                			list.add(jsonValue.getInt()+"");	
	                		}else if(jdbcType == Types.DOUBLE){
	                			list.add(((int)jsonValue.getDouble())+"");
	                		}
	                		
	                	}
	                }
	            }
			} catch (SQLException e) {
				e.printStackTrace();
			}
    	return list;
    }
    
    @SuppressWarnings({ "rawtypes", "null", "unchecked" })
		public static JSONArray extractDataMapFromQueryResult(ResultSet rs){
    		JSONArray jsonarray = new JSONArray();
    		if(rs == null){
    			return jsonarray;
    		}
		    CBResultSet cbrs =(CBResultSet) rs;
	        ResultSetMetaData rsmd;
	        int count=0;
			try {
	            while (cbrs.next())
	            {
	            	++count;
	            	rsmd = cbrs.getMetaData();
					int columnsNumber = rsmd.getColumnCount();
					JSONObject jsonObjNew = new JSONObject();
	                for (int i = 0; i < columnsNumber; i++)
	                {
	                	String type = rsmd.getColumnTypeName(i+1);
	                	String name = rsmd.getColumnName(i+1);
	                	SQLJSON jsonValue = cbrs.getSQLJSON(i+1);
	                	if(type.equals("json") && jsonValue != null){
	                		int jdbcType =jsonValue.getJDBCType();
	                		if (jdbcType == Types.JAVA_OBJECT){
		                            if(jsonValue != null){
		                            	Object o = jsonValue.getObject();
		                            	if(o != null){
		                            		if(o.getClass().equals(Map.class)){
			                            		Map m = jsonValue.getMap();
			                            		if(m.size() != 0){
			                            			jsonObjNew.putAll(m);
			                            		}
			                            	}else if(o.getClass().equals(Integer.class)){
			                            		int o_actual = (int)o;
			                            		jsonObjNew.put(name, o_actual);
			                            	}else if(o.getClass().equals(String.class)){
			                            		String o_actual = (String)o;
			                            		jsonObjNew.put(name, o_actual);
			                            	}else if(o.getClass().equals(Double.class)){
			                            		Double o_actual = (Double)o;
			                            		jsonObjNew.put(name, o_actual);
			                            	}else if(o.getClass().equals(List.class)){
			                            		List o_actual = (List) o;
			                            		jsonObjNew.put(name, o_actual);
			                            	}else if(o.getClass().equals(Float.class)){
			                            		Float o_actual = (Float) o;
			                            		jsonObjNew.put(name, o_actual);
			                            	}else if(o.getClass().equals(Byte.class)){
			                            		Byte o_actual = (Byte) o;
			                            		jsonObjNew.put(name, o_actual);
			                            	}else if(o.getClass().equals(Boolean.class)){
			                            		boolean o_actual = (boolean) o;
			                            		jsonObjNew.put(name, o_actual);
			                            	}else{
			                            		jsonObjNew.put(name, o.toString());
			                            	}
		                            	} 	
		                            }    
	                		}	
	                	}else if(jsonValue != null){
	                		int jdbcType =jsonValue.getJDBCType();
	                		if(jsonValue != null){
		                		if(Types.INTEGER == jdbcType){
		                			jsonObjNew.put(name, jsonValue.getInt());
		                		}else if(Types.ARRAY == jdbcType){
		                			jsonObjNew.put(name, jsonValue.getArray());
		                		}else if(Types.BIGINT == jdbcType){
		                			jsonObjNew.put(name, jsonValue.getBigDecimal());
		                		}else if(Types.BOOLEAN == jdbcType){
		                			jsonObjNew.put(name, jsonValue.getBoolean());
		                		}else if(Types.CHAR == jdbcType){
		                			jsonObjNew.put(name, jsonValue.getCharacterStream().toString());
		                		}else if(Types.FLOAT == jdbcType){
		                			jsonObjNew.put(name, jsonValue.getFloat());
		                		}else if(Types.BIT == jdbcType){
		                			jsonObjNew.put(name, jsonValue.getByte());
		                		}else{
		                			Object o =  jsonValue.getObject();
		                			if(o != null){
		                				jsonObjNew.put(name, o.toString());
		                			}
		                		}
	                	}
	                	}}
	                	jsonarray.add(jsonObjNew);
	            }
			} catch (SQLException e) {
				e.printStackTrace();
			} catch(Exception e){
				e.printStackTrace();
			}
		System.out.println(" number of rows returned "+count);
		return jsonarray;
	}
    
    
    public static boolean compareList(LinkedList<String> actual, LinkedList<String> expected){
    	for(String o:actual){
    		if(!expected.contains(o)){
    			return false;
    		}
    	}
    	return true;
    }
    
    /****
     * Run query without returning result set
     * @param query
     * @throws SQLException
     */
    public static void runQueryWithoutResult(String query) throws SQLException
    {
    	JDBCTestUtils.resetConnection();
        try ( Connection con = JDBCTestUtils.con)
        {
            try (Statement stmt = con.createStatement())
            {
                stmt.execute(query);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }
    
    /****
     * Create Secondary indexes from information 
     * @param indexDefinitionMap
     */
    public static void createIndexFromList(HashMap<String, HashMap<String, String>> indexDefinitionMap)
    {
    	for(String key: indexDefinitionMap.keySet()){
    		 HashMap<String, String> map = indexDefinitionMap.get(key);
    		 String indexDefinition = map.get("definition");
    		 String IndexName = map.get("name");
    		 try {
    			System.out.println("creating index "+IndexName);
				JDBCTestUtils.runQueryWithoutResult(indexDefinition);
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
    	}
    }
    
    /***
     * Create Secondary Indexes from input file information
     * @param filePath
     */
    public static void createIndexesFromDefinitions(String filePath)
    {
    	try {
			LinkedList<String>  list = JDBCTestUtils.extractIndexInformationFromFile(filePath);
			for(String definition : list){
				System.out.println(definition);
				JDBCTestUtils.runQueryWithoutResult(definition);
			}
		} catch (FileNotFoundException e) {	
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
    	
    	
    }
    
    @SuppressWarnings("finally")
	public static TestResultAnalysis runQueriesFromScenarioFileQueriesWithAggregateFunctions(String filePath){
    	LinkedList<QueryInfo> queryInfoList = null;
    	TestResultAnalysis analysis = new TestResultAnalysis();
		try {
			queryInfoList = JDBCTestUtils.extractScenariosFromFile(filePath);
			System.out.println(" Will run queries : "+queryInfoList.size());
			for(QueryInfo queryInfo: queryInfoList){
				String n1ql = queryInfo.n1ql;
				JSONArray jsonArray  =  queryInfo.expectedResult;
				analysis.totalQueries++;
				try {
					System.out.println(" ++++++++++ Run Query Number "+analysis.totalQueries+" ++++++++++++ ");
					System.out.println(n1ql);
					LinkedList<String> actualResult = JDBCTestUtils.runQuery(n1ql);
					LinkedList<String> expectedResult = JDBCTestUtils.extractDataIntoRowsAndColumns(jsonArray);
					StringBuilder errorSb = new StringBuilder();
					if(actualResult.size() != expectedResult.size()){
						errorSb.append(String.format("\n actual result size = %d != expected result size %d",actualResult.size(), expectedResult.size()));
						errorSb.append(String.format("\n actual result :: %s != expected result ::  %s",actualResult.toString(), expectedResult.toString()));
					}else{
						if(!JDBCTestUtils.compareList(actualResult,expectedResult)){
							errorSb.append(String.format(" actual result :: %s != expected result ::  %s",actualResult.toString(), expectedResult.toString()));
								
						}else{
							analysis.totalPassResults++;
						}
					}
					if(errorSb.length() > 0){
						errorSb.append("\n N1QL Query::"+n1ql);
						analysis.addFailureResult(errorSb.toString());
					}
				} catch (SQLException e) {
					
					e.printStackTrace();
				}
	    	}
		} catch (IOException | ParseException e) {
			
			e.printStackTrace();
		}catch (Exception e1){
			e1.printStackTrace();
		}finally{
			return analysis;
		}
    	
    }
    
    
    public static boolean compareJSONArray(JSONArray array1, JSONArray array2){
    	HashMap<String, JSONObject> m1 = extractJSONMapFromJSONArray(array1);
    	HashMap<String, JSONObject> m2 = extractJSONMapFromJSONArray(array1);
    	for(String key:m1.keySet()){
    		if(!m2.containsKey(key)){
    			return false;
    		}else{
    			if(!m1.get(key).equals(m2.get(key))){
    				return false;
    			}
    		}
    	}
    	return true;
    }
    
    public static HashMap<String, JSONObject> extractJSONMapFromJSONArray(JSONArray array){
    	HashMap<String, JSONObject> map = new HashMap<String, JSONObject>();
    	for(Object obj:array){
    		JSONObject o = (JSONObject) obj;
    		StringBuffer key = new StringBuffer();
    		for(Object k: o.keySet()){
    			key.append(k);
    		}
    		map.put(key.toString(), o);
    	}
    	return map;
    }
    
    @SuppressWarnings("finally")
	public static TestResultAnalysis runQueriesFromScenarioFileQueriesWithSelectFields(String filePath){
    	LinkedList<QueryInfo> queryInfoList = null;
    	TestResultAnalysis analysis = new TestResultAnalysis();
		try {
			queryInfoList = JDBCTestUtils.extractScenariosFromFile(filePath);
			System.out.println(" Will run queries : "+queryInfoList.size());
			for(QueryInfo queryInfo: queryInfoList){
				String n1ql = queryInfo.n1ql;
				JSONArray jsonArray  =  queryInfo.expectedResult;
				analysis.totalQueries++;
				try {
					System.out.println(" ++++++++++ Run Query Number "+analysis.totalQueries+" ++++++++++++ ");
					System.out.println(n1ql);
					JSONArray actualResult = JDBCTestUtils.runQueryAndExtractMap(n1ql);
					JSONArray expectedResult = JDBCTestUtils.extractDataIntoRowsAndColumnsAsMap(jsonArray);
					StringBuilder errorSb = new StringBuilder();
					if(actualResult.size() == 1 && actualResult.get(0).toString().equals("{}")){
						actualResult = new JSONArray();
					}
					if(actualResult.size() != expectedResult.size()){
						errorSb.append(String.format("\n actual result size = %d != expected result size %d",actualResult.size(), expectedResult.size()));
						errorSb.append(String.format("\n actual result :: %s != expected result ::  %s",actualResult.toString(),expectedResult.toString()));
					}else{
						try{
						actualResult = JDBCTestUtils.sortJsonArray(actualResult);
						expectedResult = JDBCTestUtils.sortJsonArray(expectedResult);
						}catch(Exception e){
							e.printStackTrace();
						}
						if(!compareJSONArray(actualResult,expectedResult)){
							errorSb.append(String.format(" actual result :: %s != expected result ::  %s",actualResult.toString(),expectedResult.toString()));
								
						}else{
							analysis.totalPassResults++;
						}
					}
					if(errorSb.length() > 0){
						errorSb.append("\n N1QL Query::"+n1ql);
						analysis.addFailureResult(errorSb.toString());
					}
				} catch (SQLException e) {
					
					e.printStackTrace();
				}
	    	}
		} catch (IOException | ParseException e) {
			
			e.printStackTrace();
		}catch (Exception e1){
			e1.printStackTrace();
		}finally{
			return analysis;
		}
    	
    }
    
    @SuppressWarnings("unchecked")
	public static JSONArray sortJsonArray(JSONArray array) {
    	if(array.size() == 0){
    		return array;
    	}
        ArrayList<JSONObject> jsons = new ArrayList<JSONObject>();
        for (int i = 0; i < array.size(); i++) {
            jsons.add((JSONObject)array.get(i));
        }
        for(Object field:((JSONObject)array.get(0)).keySet()){
        	final Object field_val = field;
	        Collections.sort(jsons, new Comparator<JSONObject>() {
	        	@Override
	            public int compare(JSONObject lhs, JSONObject rhs) {
	                String lid = (String) lhs.get(field_val);
	                String rid = (String) rhs.get(field_val);
	                // Here you could parse string id to integer and then compare.
	                return lid.compareTo(rid);
	            }
        
        });}
        JSONArray returnValue = new JSONArray();
        returnValue.addAll(jsons);
        return returnValue;
    }
    
    /***
     * Extract Scenario from input file
     * @param filePath
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     * @throws ParseException
     */
    public static LinkedList<QueryInfo> extractScenariosFromFile(String filePath) 
    		throws FileNotFoundException, IOException, ParseException{
    	String line;
	    LinkedList<QueryInfo> ll = new LinkedList<QueryInfo>();
	    JSONParser parser = new JSONParser();
    	try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {  
    	    try{
	    	    while ((line = br.readLine()) != null) {
	    	       JSONObject json = (JSONObject) parser.parse(line);
	    	       String n1ql = (String) json.get("n1ql");
	    	       String sql = (String) json.get("sql");
	    	       JSONArray expectedResult = (JSONArray) json.get("expected_result");
	    	       QueryInfo info = new QueryInfo(n1ql, sql, expectedResult);
	    	       ll.add(info);
	    	    }
    	    }catch(IOException ex){
    	    	System.out.println(ex.toString());
    	    }catch(Exception e){
    	    	System.out.println(e.toString());
    	    }
    	}
    	return ll;
    }
    
    /***
     * Method to create primary indexes
     * @param bucketNames
     */
    public static void createPrimaryIndexes(Set<String> bucketNames){
    	for(String bucketName:bucketNames){
    		String query = "create primary index on "+bucketName;
    		try {
				JDBCTestUtils.runQueryWithoutResult(query);
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
    	}
    }
 
    /***
     * Extract Data Information from file
     * @param filePath
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     * @throws ParseException
     */
    public static HashMap<String, HashMap<String, JSONObject>> extractDataInformationFromFile(String filePath) 
    		throws FileNotFoundException, IOException, ParseException{
    	JSONParser parser = new JSONParser();
    	HashMap<String, HashMap<String, JSONObject>> bucketDataMap = new HashMap<String, HashMap<String, JSONObject>>();
    	JDBCTestUtils.extractFolder(filePath);
    	String dataDumpPath=filePath.replace(".zip", "");
    	File folder = new File(dataDumpPath);
    	for(File file : folder.listFiles()){
    		Object obj = parser.parse(new FileReader(file.getAbsolutePath()));
            JSONObject jsonObject = (JSONObject) obj;
            HashMap<String, JSONObject> dataMap = new HashMap<String, JSONObject>();
            for(Object key : jsonObject.keySet()){
            	JSONObject jsonValue = (JSONObject) jsonObject.get(key.toString());
            	dataMap.put(key.toString(), jsonValue);
            }
            bucketDataMap.put(file.getName().replace(".txt", ""), dataMap);   
    	}
    	return bucketDataMap;
    }
    
    /****
     * INSERT Data in server
     * @param map
     * @param bucketName
     */
    public static void insertData(HashMap<String, JSONObject> map, String bucketName){
    	for(String key : map.keySet()){
    		JSONObject obj = map.get(key);
    		String query = "INSERT INTO "+bucketName+" (KEY, VALUE) VALUES (\""+key+"\","+obj+")";
    		try {
				JDBCTestUtils.runQueryWithoutResult(query);
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
    	}
    }
    
    /****
     * BULK INSERT Data in server
     * @param map
     * @param bucketName
     */
    public static void  bulkInsertData(HashMap<String, JSONObject> map, String bucketName){
    	StringBuffer sb = new StringBuffer();
    	int count = 0;
    	for(String key : map.keySet()){
    		sb.append(String.format("(%s,%s)",key,map.get(key)));
    		++count;
    		if(count < map.size() ){
    			sb.append(",");
    		}	
    	}
    	String query = "INSERT INTO "+bucketName+" (KEY, VALUE) VALUES "+sb.toString();
    	try {
				JDBCTestUtils.runQueryWithoutResult(query);
		} catch (SQLException e) {
				e.printStackTrace();
		}
    }
    
    /****
     * BULK UPSERT Data in server
     * @param map
     * @param bucketName
     */
    public static void  bulkUpsertData(HashMap<String, JSONObject> map, String bucketName){
    	StringBuffer sb = new StringBuffer();
    	int count = 0;
    	for(String key : map.keySet()){
    		sb.append(String.format("(%s,%s)",key,map.get(key)));
    		++count;
    		if(count < map.size() ){
    			sb.append(",");
    		}	
    	}
    	String query = "UPSERT INTO "+bucketName+" (KEY, VALUE) VALUES "+sb.toString();
    	try {
				JDBCTestUtils.runQueryWithoutResult(query);
		} catch (SQLException e) {
				e.printStackTrace();
		}
    }
    
    /***
     * UPSERT Data in server
     * @param map
     * @param bucketName
     */
    public static void upsertData(HashMap<String, JSONObject> map, String bucketName){
    	for(String key : map.keySet()){
    		JSONObject obj = map.get(key);
    		String query = "UPSERT INTO "+bucketName+" (KEY, VALUE) VALUES (\""+key+"\","+obj+")";
    		try {
				JDBCTestUtils.runQueryWithoutResult(query);
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
    	}
    }
    
    @SuppressWarnings("unchecked")
	public static JSONArray extractDataIntoRowsAndColumnsAsMap(JSONArray jsonArray){
    	JSONArray listOfObjects = new JSONArray();
    	for(Object obj:jsonArray){
    		HashMap<String, String> m = new HashMap<String, String>();
    		JSONObject jsonObj = (JSONObject) obj;
    		for(Object key:jsonObj.keySet()){
    			Object value = jsonObj.get(key);
    			if(value != null){
    				m.put(key.toString(), value.toString());
    			}
    		}
    		JSONObject jobj = new JSONObject();
    		jobj.putAll(m);
    		listOfObjects.add(jobj);
    	}
    	return listOfObjects;
    }
    
    /**** 
     * Convert JSONArray of JSON objects into list of String values
     * @param jsonArray
     * @return
     */
    public static LinkedList<String> extractDataIntoRowsAndColumns(JSONArray jsonArray){
    	LinkedList<String> listOfObjects = new LinkedList<String>();
    	for(Object obj:jsonArray){
    		JSONObject jsonObj = (JSONObject) obj;
    		for(Object key:jsonObj.keySet()){
    			Object value = jsonObj.get(key);
    			if(value != null){
    				listOfObjects.add(value.toString());
    			}
    		}
    	}
    	return listOfObjects;
    }
    
    /****
     * Extract Folder
     * @param zipFile
     * @throws ZipException
     * @throws IOException
     */
    static public void extractFolder(String zipFile) throws ZipException, IOException 
    {
        System.out.println(zipFile);
        int BUFFER = 2048;
        File file = new File(zipFile);

        ZipFile zip = new ZipFile(file);
        String newPath = zipFile.substring(0, zipFile.length() - 4);

        new File(newPath).mkdir();
        Enumeration zipFileEntries = zip.entries();

        // Process each entry
        while (zipFileEntries.hasMoreElements())
        {
            // grab a zip file entry
            ZipEntry entry = (ZipEntry) zipFileEntries.nextElement();
            String currentEntry = entry.getName();
            File destFile = new File(newPath, currentEntry);
            //destFile = new File(newPath, destFile.getName());
            File destinationParent = destFile.getParentFile();

            // create the parent directory structure if needed
            destinationParent.mkdirs();

            if (!entry.isDirectory())
            {
                BufferedInputStream is = new BufferedInputStream(zip
                .getInputStream(entry));
                int currentByte;
                // establish buffer for writing file
                byte data[] = new byte[BUFFER];

                // write the current file to disk
                FileOutputStream fos = new FileOutputStream(destFile);
                BufferedOutputStream dest = new BufferedOutputStream(fos,
                BUFFER);

                // read and write until last byte is encountered
                while ((currentByte = is.read(data, 0, BUFFER)) != -1) {
                    dest.write(data, 0, currentByte);
                }
                dest.flush();
                dest.close();
                is.close();
            }

            if (currentEntry.endsWith(".zip"))
            {
                // found a zip file, try to open
                extractFolder(destFile.getAbsolutePath());
            }
        }
    }
    
    /****
     * Return Index information from file
     * @param filePath
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     * @throws ParseException
     */
    public static LinkedList<String> extractIndexInformationFromFile(String filePath) 
    		throws FileNotFoundException, IOException, ParseException{
    	JSONParser parser = new JSONParser();
    	LinkedList<String> ll = new LinkedList<String>();
    	Object obj = parser.parse(new FileReader(filePath));
        JSONObject jsonObject = (JSONObject) obj;
        for(Object key : jsonObject.keySet()){
        	JSONObject jsonObj = (JSONObject) jsonObject.get(key.toString());
        	for(Object indexName : jsonObj.keySet()){
        		JSONObject indexMap = (JSONObject) jsonObj.get(indexName);
        		String definition = (String) indexMap.get("definition");
        		ll.add(definition);
        	}
        }
        System.out.println(ll.toString());
        return ll;
    }
    
    /***
     * Run queries with method like aggregate
     * @param clusterInfo
     * @param inputPath
     */
    public static TestResultAnalysis runTestsWithAggregateFunctionQueries(
    		ClusterInfo clusterInfo, 
    		String inputPath){
    	String scenarioFilePath = JDBCTestUtils.initializeCluster(clusterInfo, inputPath);
        System.out.println(" Running Queries ");
        System.out.println(scenarioFilePath);
		TestResultAnalysis analysis = JDBCTestUtils.runQueriesFromScenarioFileQueriesWithAggregateFunctions(scenarioFilePath);
		analysis.publishResult();
		return null;
    }
    
    /****
     * Run queries that return array of JSONObjects
     * @param clusterInfo
     * @param inputPath
     */
    public static TestResultAnalysis runTestsWithFieldProjectionQueries(
    		ClusterInfo clusterInfo,
    		String inputPath){
    	String scenarioFilePath = JDBCTestUtils.initializeCluster(clusterInfo, inputPath);
        System.out.println(" Running Queries ");
		TestResultAnalysis analysis = JDBCTestUtils.runQueriesFromScenarioFileQueriesWithSelectFields(scenarioFilePath);
		analysis.publishResult();
		return analysis;
    }
    
    public static void deleteDataFromBucket(String bucket){
    	String query = "delete from "+bucket;
    	try{
    		runQueryWithoutResult(query);
    	}catch(SQLException e){
    		e.printStackTrace();
    	}
    	
    }
    
    @SuppressWarnings("unchecked")
	public static void main(String[] args) throws SQLException
    {
    	JSONArray array = new JSONArray();
    	JSONObject obj1 = new JSONObject();
    	JSONObject obj2 = new JSONObject();
    	JSONObject obj3 = new JSONObject();
    	obj1.put("name", "z");
    	obj1.put("another", "a");
    	obj2.put("name", "a");
    	obj2.put("another", "c");
    	obj3.put("name", "c");
    	obj3.put("another", "m");
    	array.add(obj1);
    	array.add(obj2);
    	array.add(obj3);
    	array = JDBCTestUtils.sortJsonArray(array);
    	System.out.println(array.toJSONString());
        
    }
}
