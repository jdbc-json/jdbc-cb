package com.couchbase.jdbc;

import java.util.HashMap;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class ClusterInfo {
	NodeInfo global ;
	NodeInfo masterNodeInfo ;
	public HashMap<String, NodeInfo> nodeInformation;
	public HashMap<String, BucketInfo> bucketInformation;
	
	
	
	/***
	 * Constructor
	 */
	public ClusterInfo() {
		global = new NodeInfo();
		masterNodeInfo = null;
		this.nodeInformation = new HashMap<String, NodeInfo>();
		this.bucketInformation = new HashMap<String, BucketInfo>();
	}
	

	/***
	 * Add Node information
	 * @param nodeInfo
	 */
	public void addNodeInfo(NodeInfo nodeInfo){
		if(nodeInformation == null){
			nodeInformation = new HashMap<String, NodeInfo>();
		}
		nodeInformation.put(String.format("%s:%s", nodeInfo.ip,nodeInfo.port), nodeInfo);
	}
	
	/***
	 * Add Bucket Information
	 * @param bucketInfo
	 */
	public void addBucketInfo(BucketInfo bucketInfo){
		this.bucketInformation.put(bucketInfo.name, bucketInfo);
	}
	
	/***
	 * Reset Bucket Information
	 */
	public void resetBucketInformation(){
		new HashMap<String, BucketInfo>();
	}
	
	@SuppressWarnings("unchecked")
	public JSONObject createJSONObject(){
		JSONObject jsonObject = new JSONObject();
		JSONArray nodeInformationObjectArray = new JSONArray();
		for(NodeInfo nodeInfo:this.nodeInformation.values()){
			nodeInformationObjectArray.add(nodeInfo.createJSONObject());
		}
		jsonObject.put("node_info", nodeInformationObjectArray);
		JSONArray bucketInformationObjectArray = new JSONArray();
		for(BucketInfo bucketInfo:this.bucketInformation.values()){
			bucketInformationObjectArray.add(bucketInfo.createJSONObject());
		}
		jsonObject.put("bucket_info", bucketInformationObjectArray);
		jsonObject.put("global", global.createJSONObject());
		return jsonObject;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("\n ________________ CLUSTER INFORMATION _____________________");
		sb.append("\n ________________ Bucket Information _____________________");
		for(BucketInfo info:bucketInformation.values()){
			sb.append("\n "+info.toString());
		}
		sb.append("\n ________________ Cluster Information _____________________");
		for(NodeInfo info:nodeInformation.values()){
			sb.append("\n "+info.toString());
		}
		return sb.toString();
	}
}
