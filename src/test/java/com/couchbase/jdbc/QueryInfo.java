package com.couchbase.jdbc;
import org.json.simple.JSONArray;

public class QueryInfo {
	public String n1ql;
	public String sql;
	public JSONArray expectedResult;
	public QueryInfo(String n1ql, String sql, JSONArray expectedResult) {
		this.n1ql = n1ql;
		this.sql = sql;
		this.expectedResult = expectedResult;
	}
	
}
