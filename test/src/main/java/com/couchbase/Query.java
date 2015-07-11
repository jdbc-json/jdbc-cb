package com.couchbase;

/**
 * Created by davec on 15-07-07.
 */
import java.sql.*;

public class Query {
	static String JDBCDriver = "com.couchbase.CBDriver";

	static String ConnectionURL = "jdbc:couchbase://54.237.32.30:8093";

	public static void main(String[] args) {
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;

		String query = "SELECT * FROM customer LIMIT 1";
		try {
			con = DriverManager.getConnection(ConnectionURL);
			stmt = con.createStatement();
			rs = stmt.executeQuery(query);

            DatabaseMetaData metadata = con.getMetaData();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            rsmd = rs.getMetaData();
            columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i < columnsNumber; i++) {
                    System.out.println(rsmd.getColumnName(i) + ":"
                    + rs.getString(i) + " ");
                }
                System.out.println();
            }
		} catch (Exception e) {
		e.printStackTrace();
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
			} catch (SQLException se1) {
			}
			try {
				if (stmt != null) {
					stmt.close();
				}
			} catch (SQLException se2) {
			}
			try {
				if (con != null) {
					con.close();
				}
			} catch (SQLException se4) {
				se4.printStackTrace();
			}
		}
	}
}
