package com.couchbase.jdbc;

import org.json.simple.JSONObject;

public class BucketInfo {
	public String name;
	public int port;
	public int ramQuotaMB;
	public int replicaNumber;
	public Integer proxyPort;
	public String authType;
	public static int proxy_port = 21000;
	
	/***
	 * Constructor
	 */
	public BucketInfo(){
		this.name = "default";
		this.port = 12000;
		this.ramQuotaMB = 1000;
		this.replicaNumber = 0;
		this.proxyPort = BucketInfo.proxy_port++;
		this.authType = "none";
	}
	
	/***
	 * Constructor
	 * @param name
	 * @param port
	 * @param ramQuotaMB
	 * @param replicaNumber
	 * @param proxyPort
	 * @param authType
	 */
	public BucketInfo(
			String name, 
			int port, 
			int ramQuotaMB, 
			int replicaNumber, 
			int proxyPort, 
			String authType) {
		this.name = name;
		this.port = port;
		this.ramQuotaMB = ramQuotaMB;
		this.replicaNumber = replicaNumber;
		this.proxyPort = proxyPort;
		this.authType = authType;
	}
	
	@SuppressWarnings("unchecked")
	public JSONObject createJSONObject(){
		JSONObject obj = new JSONObject();
		obj.put("name", this.name);
		obj.put("authType", this.authType);
		obj.put("proxyPort", this.proxyPort);
		obj.put("ramQuotaMB", this.ramQuotaMB);
		obj.put("replicaNumber", this.replicaNumber);
		return obj;
	}

	@Override
	public String toString() {
		return "\n [name=" + name + ", port=" + port + ", ramQuotaMB=" + ramQuotaMB + ", replicaNumber="
				+ replicaNumber + ", proxyPort=" + proxyPort + ", authType=" + authType + "]";
	}

}
