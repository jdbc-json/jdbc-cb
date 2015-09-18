package com.couchbase.jdbc.test;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.couchbase.CBResultSet;
import com.couchbase.json.SQLJSON;

@RunWith(JUnit4.class)
public class N1QLErrorJDBCDriverHandlingTests {
	static ClusterInfo clusterInfo = null;
	
	@BeforeClass
	public static void openConnection() throws Exception
	{
		JDBCTestUtils.setConnection();
		String clusterConfigPath = "/tmp/config.json";
		N1QLErrorJDBCDriverHandlingTests.clusterInfo = ClusterSetupUtils.readConfigFile(clusterConfigPath);
		ClusterSetupUtils.initializeCluster(N1QLErrorJDBCDriverHandlingTests.clusterInfo);
		ClusterSetupUtils.createBuckets(N1QLErrorJDBCDriverHandlingTests.clusterInfo);
		Thread.sleep(5000);
	}
	
	@AfterClass
	public static void closeConnection() throws Exception
	{
		ClusterSetupUtils.deleteBuckets(N1QLErrorJDBCDriverHandlingTests.clusterInfo);
	}
	
	@Test
	public void testQueryMissingBucket(){
		
		String query = "select * from does_not_exist_bucket";
		JDBCTestUtils.setConnection();
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
		JDBCTestUtils.setConnection();
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
	public void testQueryTooManyParallelQueries(){
		
	}
	
	@Test
	public void testQueryWithWrongProjectionsTypes(){
		JDBCTestUtils.setConnection();
		JDBCTestUtils.createPrimaryIndexes(N1QLErrorJDBCDriverHandlingTests.clusterInfo.bucketInformation.keySet());
		String query = "select 1";
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
		                jsonVal1.getMap();
		             
                	}
                }
            }
        } catch (SQLException e1) {
			// TODO Auto-generated catch block
			assertEquals("xx",e1.getMessage());
		}
        String drop_primary_index = "drop primary index on default";
        try {
			JDBCTestUtils.runQueryWithoutResult(drop_primary_index);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void testQueryWithIncorrectSQLSyntax(){
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		JDBCTestUtils.createPrimaryIndexes(N1QLErrorJDBCDriverHandlingTests.clusterInfo.bucketInformation.keySet());
		String query = "crap this is wrong syntax";
		JDBCTestUtils.setConnection();
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
	
	@SuppressWarnings("resource")
	@Test
	public void testQueryWithLargePayLoadForQueries(){
		
	}
	

}
