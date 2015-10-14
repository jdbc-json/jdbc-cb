package com.couchbase.jdbc;

import java.util.ArrayList;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class NodeInfo {
	public String ip;
	public int port;
	public boolean isMasterNode;
	public int n1qlPort;
	public int indexPort;
	public String membaseUserId;
	public String membasePassword;
	public String machineUserId;
	public String machinePassword;
	public ArrayList<String> services;
	
	/***	
	 * Constructor
	 */
	public NodeInfo(){
		this.ip = "127.0.0.1";
		this.indexPort = 9102;
		this.isMasterNode = false;
		this.machineUserId = "root";
		this.machinePassword  = "couchbase";
		this.membaseUserId = "Administrator";
		this.membasePassword = "password";
		this.n1qlPort = 8093;
		this.port = 8091;
		services = new ArrayList<String>();
		services.add("kv");
		services.add("n1ql");
		services.add("index");
	}
	
	/***
	 * Constructor
	 * @param ip
	 * @param port
	 * @param n1qlPort
	 * @param indexPort
	 * @param membaseUserId
	 * @param membasePassword
	 * @param machineUserId
	 * @param machinePassword
	 * @param services
	 */
	public NodeInfo(
			String ip, 
			int port, 
			int n1qlPort, 
			int indexPort,
			String membaseUserId, 
			String membasePassword, 
			String machineUserId, 
			String machinePassword,
			ArrayList<String> services) {
		this.ip = ip;
		this.port = port;
		this.n1qlPort = n1qlPort;
		this.indexPort = indexPort;
		this.membaseUserId = membaseUserId;
		this.membasePassword = membasePassword;
		this.machineUserId = machineUserId;
		this.machinePassword = machinePassword;
		this.services = services;
	}
	
	public String getServerURL(){
		return String.format("http://%s:%s",this.ip,this.port);
	}
	
	public String getQueryServerURL(){
		return String.format("jdbc:couchbase://%s:%s",this.ip,this.n1qlPort);
	}
	
	/***
	 * Create JSON Object from the current object 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public JSONObject createJSONObject(){
		JSONObject obj  = new JSONObject();
		obj.put("ip", this.ip);
		obj.put("port", this.port);
		obj.put("n1qlPort", this.n1qlPort);
		obj.put("indexPort", this.indexPort);
		obj.put("membaseUserId", this.membaseUserId);
		obj.put("membasePassword", this.membasePassword);
		obj.put("machineUserId", this.machineUserId);
		obj.put("machinePassword", this.machinePassword);
		JSONArray array = new JSONArray();
		for(String service:services){
			array.add(service);
		}
		obj.put("services", array);
		return obj;
	}
	
	/***
	 * Method to return comma delimited services
	 * @return
	 */
	public String getServices(){
		StringBuffer sb = new StringBuffer();
		if(this.services.size() > 0){
			sb.append(this.services.toString().replace("[", "").replace("]", ""));
		}else{
			sb.append("kv");
		}
		return sb.toString().replace(" ", "");
	}

	@Override
	public String toString() {
		return "\n [ip=" + ip + ", port=" + port + ",  n1qlPort="
				+ n1qlPort + ", indexPort=" + indexPort + ", membaseUserId=" + membaseUserId + ", membasePassword="
				+ membasePassword + ", machineUserId=" + machineUserId + ", machinePassword=" + machinePassword
				+ ", services=" + services + "]";
	}

	
	
	
	

}
