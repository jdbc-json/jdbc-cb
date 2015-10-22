package com.couchbase.jdbc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.Proxy;
import java.net.InetSocketAddress;
import java.io.OutputStreamWriter;
import com.couchbase.jdbc.ClusterInfo;
import com.couchbase.jdbc.NodeInfo;


public class ClusterSetupUtils {
	
			/***
			 * Rebalance-In Nodes
			 */
			public static void rebalanceIn(ClusterInfo clusterInfo){
				NodeInfo masterNodeInfo = null;
				try{
					// Node addition Phase
					for(NodeInfo nodeInfo:clusterInfo.nodeInformation.values()){
						if(nodeInfo.isMasterNode){
							masterNodeInfo = nodeInfo;
						}
					}
					for(NodeInfo nodeInfo:clusterInfo.nodeInformation.values()){
						if(!nodeInfo.isMasterNode){
							String services = nodeInfo.getServices();
							String curlCommand  = String.format("curl  -u %s:%s -v -X POST http://%s:%d/controller/addNode  -d hostname=%s:%s&password=%s&user=%s&services=%s",nodeInfo.membaseUserId,nodeInfo.membasePassword,masterNodeInfo.ip,masterNodeInfo.port,nodeInfo.ip,nodeInfo.port,nodeInfo.membasePassword,nodeInfo.membaseUserId,services);
							ClusterSetupUtils.runCommand(curlCommand);
							Thread.sleep(10000);
						}
					}
					// Cluster Rebalance Phase
					StringBuffer rebalanceIps = new StringBuffer();
					String masterNodeIP = "";
					int count = 0;
					for(NodeInfo nodeInfo:clusterInfo.nodeInformation.values()){
						if(nodeInfo.isMasterNode){
							masterNodeIP = String.format("n_0FORTY%s",nodeInfo.ip);
						}else{
							++count;
							rebalanceIps.append(String.format("TWENTYn_%dFORTY%s",count,nodeInfo.ip));
						}
						
					}
					String url = String.format("http://%s:%d/controller/rebalance",masterNodeInfo.ip,masterNodeInfo.port);
					String curlCommand = String.format("ejectedNodes=&knownNodes=%s%s", masterNodeIP, rebalanceIps.toString());
					ClusterSetupUtils.runCURLCommand(curlCommand.replace("FORTY","%40").replace("TWENTY","%2C"),  url,masterNodeInfo.membaseUserId,masterNodeInfo.membasePassword);
					Thread.sleep(6000);
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			
			/***
			 * Rebalance-Out Nodes
			 */
			public static void rebalanceOut(ClusterInfo clusterInfo){
				NodeInfo masterNodeInfo = null;
				try{
					// Node addition Phase
					for(NodeInfo nodeInfo:clusterInfo.nodeInformation.values()){
						if(nodeInfo.isMasterNode){
							masterNodeInfo = nodeInfo;
						}
					}
					
					// Cluster Rebalance Phase
					StringBuffer rebalanceOut = new StringBuffer();
					StringBuffer rebalanceIps = new StringBuffer();
					
					String masterNodeIP = "";
					int count = 0;
					for(NodeInfo nodeInfo:clusterInfo.nodeInformation.values()){
						if(nodeInfo.isMasterNode){
							masterNodeIP = String.format("n_0FORTY%s",nodeInfo.ip);
						}else{
							++count;
							if(count == 1){
								rebalanceOut.append(String.format("n_%dFORTY%s",count,nodeInfo.ip));
							}else{
								rebalanceOut.append(String.format("TWENTYn_%dFORTY%s",count,nodeInfo.ip));
							}
							rebalanceIps.append(String.format("TWENTYn_%dFORTY%s",count,nodeInfo.ip));
						}
						
					}
					String url = String.format("http://%s:%d/controller/rebalance",masterNodeInfo.ip,masterNodeInfo.port);
					String curlCommand = String.format("ejectedNodes=%s&knownNodes=%s%s", rebalanceOut.toString(), masterNodeIP, rebalanceIps.toString() );
					ClusterSetupUtils.runCURLCommand(curlCommand.replace("FORTY","%40").replace("TWENTY","%2C"),  url,masterNodeInfo.membaseUserId,masterNodeInfo.membasePassword);
					Thread.sleep(6000);
				}catch(Exception e){
					e.printStackTrace();
				}
			}

	
		/***
		 * Initialize Clusters
		 * @param clusterInfo
		 */
		public static void initializeCluster(ClusterInfo clusterInfo){
			for(NodeInfo nodeInfo:clusterInfo.nodeInformation.values()){
				if(nodeInfo.isMasterNode){
					clusterInfo.masterNodeInfo = nodeInfo;
					String services = nodeInfo.getServices();
					String curlCommand  = String.format("curl -X POST -u %s:%s -d username=%s -d password=%s -d services=%s http://%s:%s//node/controller/setupServices", nodeInfo.membaseUserId, nodeInfo.membasePassword, nodeInfo.membaseUserId, nodeInfo.membasePassword, services,nodeInfo.ip,nodeInfo.port);
					ClusterSetupUtils.runCommand(curlCommand);
					curlCommand  = String.format("curl  -u %s:%s -v -X POST http://%s:%d/settings/web  -d password=%s&username=%s&port=SAME",nodeInfo.membaseUserId,nodeInfo.membasePassword,nodeInfo.ip,nodeInfo.port,nodeInfo.membasePassword,nodeInfo.membaseUserId);
					ClusterSetupUtils.runCommand(curlCommand);
				}
			}
		}
		
		/***
		 * Method to create buckets
		 * @param clusterInfo
		 */
		public static void createBuckets(ClusterInfo clusterInfo){
			for(BucketInfo bucketInfo: clusterInfo.bucketInformation.values()){
				ClusterSetupUtils.createBucket(
						bucketInfo,
						clusterInfo.masterNodeInfo);
			}
			
		}
		
		/***
		 * Delete buckets
		 * @param clusterInfo
		 */
		public static void deleteBuckets(ClusterInfo clusterInfo){
			System.out.println(clusterInfo.toString());
			for(BucketInfo bucketInfo: clusterInfo.bucketInformation.values()){
				ClusterSetupUtils.deleteBucket(
						bucketInfo,
						clusterInfo.masterNodeInfo);
			}
			
		}
		
		/***
		 * Create Bucket
		 * @param userId
		 * @param password
		 * @param proxyPort
		 * @param name
		 * @param ip
		 * @param port
		 */
		public static void createBucket(BucketInfo bucketInfo, NodeInfo nodeInfo){
			String curlCommand  = String.format("curl -X POST -u %s:%s -d name=%s -d ramQuotaMB=1000 -d authType=none  -d proxyPort=%s http://%s:%s/pools/default/buckets",nodeInfo.membaseUserId, nodeInfo.membasePassword, bucketInfo.name, bucketInfo.proxyPort, nodeInfo.ip, nodeInfo.port);
			ClusterSetupUtils.runCommand(curlCommand);
		}
	
		/***
		 * Delete bucket
		 * @param userId
		 * @param password
		 * @param name
		 * @param ip
		 * @param port
		 */
		public static void deleteBucket(BucketInfo bucketInfo, NodeInfo nodeInfo){
			String curlCommand  = String.format("curl -X DELETE -u %s:%s http://%s:%s/pools/default/buckets/%s",nodeInfo.membaseUserId, nodeInfo.membasePassword, nodeInfo.ip, nodeInfo.port, bucketInfo.name);
			ClusterSetupUtils.runCommand(curlCommand);
		}
		
		/***
		 * Read Cluster Configuration file
		 * @param filePath
		 * @throws FileNotFoundException
		 * @throws IOException
		 * @throws ParseException
		 */
	    public static ClusterInfo readConfigFile(String filePath) 
	    		throws FileNotFoundException, IOException, ParseException{
	    	filePath = "target/test-classes/"+filePath;
	    	JSONParser parser = new JSONParser();
	    	ClusterInfo clusterInfo = new ClusterInfo(); 
	    	Object obj = parser.parse(new FileReader(filePath));
	        JSONObject jsonObject = (JSONObject) obj;
	        JSONObject global = (JSONObject) jsonObject.get("global");
	        JSONArray nodeInformation = (JSONArray) jsonObject.get("node_info");
	        int index_port = 0, n1ql_port = 0, port = 0;
	        String machineUserId = null,machinePassword = null,membaseUserId = null,membasePassword = null;
	        ArrayList<String> services = new ArrayList<String>();
	        
	        // Initialize Bucket Information
	        if (jsonObject.containsKey("bucket_info")){
	        	JSONArray bucketInfoArray = (JSONArray)jsonObject.get("bucket_info");
	        	for(int j = 0;j<bucketInfoArray.size();++j){
	        		JSONObject bucketInf = (JSONObject) bucketInfoArray.get(j);
	        		BucketInfo bucketInfo = new BucketInfo();
	        		if(bucketInf.containsKey("name")){
	        			bucketInfo.name = (String)bucketInf.get("name");
	        		}
	        		if(bucketInf.containsKey("port")){
	        			bucketInfo.port = (int)bucketInf.get("port");
	        		}
	        		if(bucketInf.containsKey("replicaNumber")){
	        			bucketInfo.replicaNumber = (int)(long)bucketInf.get("replicaNumber");
	        		}
	        		if(bucketInf.containsKey("ramQuotaMB")){
	        			bucketInfo.ramQuotaMB = (int)(long)bucketInf.get("ramQuotaMB");
	        		}
	        		if(bucketInf.containsKey("authType")){
	        			bucketInfo.authType = (String)bucketInf.get("authType");
	        		}
	        		if(bucketInf.containsKey("proxyPort")){
	        			Object obj1 = bucketInf.get("proxyPort");
	        			if(obj1 != null){
	        				bucketInfo.proxyPort = (int)(long)obj1;
	        			}
	        		}
	        		clusterInfo.addBucketInfo(bucketInfo);
	        	}
	        }
	       
	        // Initialize Global Information
	        if(global.containsKey("machineUserid")){
	        	machineUserId = (String) global.get("machineUserid");
	        }
	        if(global.containsKey("machinePassword")){
	        	machinePassword = (String) global.get("machinePassword");
	        }
	        if(global.containsKey("membaseUserId")){
	        	membaseUserId = (String) global.get("membaseUserId");
	        }
	        if(global.containsKey("membasePassword")){
	        	membasePassword = (String) global.get("membasePassword");
	        }
	        if(global.containsKey("index_port")){
	        	index_port = (int) global.get("index_port");
	        }
	        if(global.containsKey("port")){
	        	port = (int) (long) global.get("port");
	        }
	        if(global.containsKey("n1qlPort")){
	        	n1ql_port = (int) (long) global.get("n1qlPort");
	        }
	        if(global.containsKey("services")){
	        	JSONArray services_array = (JSONArray) global.get("services");
	        	for(int j  = 0;j<services_array.size();++j){
	        		services.add((String)services_array.get(j));
	        	}
	        }
	        
	        // Initialize Per Node Information
	        for(int i =0;i< nodeInformation.size();++i){
	        	NodeInfo info = new NodeInfo();
	        	JSONObject nodeInfo = (JSONObject) nodeInformation.get(i);
	        	info.ip = (String) nodeInfo.get("ip");
	        	if(info.ip.equals("127.0.0.1") || info.ip.equals("localhost")){
	        		info.ip = getLocalHostIPAdress(); 
	        	}
	        	info.machineUserId = machineUserId;
	        	info.machinePassword = machinePassword;
	        	info.machinePassword = (String) nodeInfo.get("machinePassword");
	        	info.membaseUserId = membaseUserId;
	        	info.membasePassword = membasePassword;
	        	if(nodeInfo.containsKey("n1qlPort")){
	        		info.n1qlPort = (int) (long) nodeInfo.get("n1qlPort");
	        	}else{
	        		info.n1qlPort  = n1ql_port;
	        	}
	        	if(nodeInfo.containsKey("port")){
	        		info.port = (int) (long) nodeInfo.get("port");
	        	}else{
	        		info.port = port;
	        	}
	        	if(nodeInfo.containsKey("isMasterNode")){
	        		info.isMasterNode = Boolean.parseBoolean((String)nodeInfo.get("isMasterNode"));
	        	}
	        	if(nodeInfo.containsKey("indexPort")){
	        		info.indexPort = (int) (long) nodeInfo.get("indexPort");
	        	}else{
	        		info.indexPort = index_port;
	        	}
	        	if(nodeInfo.containsKey("services")){
	        		JSONArray local_services = (JSONArray) nodeInfo.get("services");
	        		ArrayList<String> service_info = new ArrayList<String>();
	        		for(int j=0;j<local_services.size();++j){
	        			service_info.add((String)local_services.get(j));
	        		}
	        		info.services = service_info;
	        	}else{
	        		info.services = services;
	        	}
	        	clusterInfo.addNodeInfo(info);
	        }
	        return clusterInfo;
	    }
		
	    /***
	     * Run curl command
	     * @param command
	     */
		public static void runCommand(String command){
			Process p = null;
			try {
				
				System.out.println(command);
				p = Runtime.getRuntime().exec(command);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = null; 
			     try {
			        while ((line = input.readLine()) != null)
			            System.out.println(line);
			     } catch (IOException e) {
			            e.printStackTrace();
			     }
			try {
				p.waitFor();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		
		/***
	     * Run curl command
	     * @param command
	     */
		public static void runCommandArray(String[] command){
			Process p = null;
			try {
				p = Runtime.getRuntime().exec(command);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = null; 
			     try {
			        while ((line = input.readLine()) != null)
			            System.out.println(line);
			     } catch (IOException e) {
			            e.printStackTrace();
			     }
			try {
				p.waitFor();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		
		/***
		 * Get Local IP Address
		 */
		private static String getLocalHostIPAdress(){
			String ipAddr = "";
			try{
				InetAddress inetAddr = InetAddress.getLocalHost();
				byte[] addr = inetAddr.getAddress();
				// Convert to dot representation
				
				for (int i = 0; i < addr.length; i++) {
					 if (i > 0) {
						 ipAddr += ".";
					 }
					 ipAddr += addr[i] & 0xFF;
				}
			}catch(Exception e){
				e.printStackTrace();
			}
			return ipAddr;
		}
		
		/***
		 * Run Curl command by creating a script
		 */
		public static void runCURLCommand(String params, String url, String user, String password){
			try {
					String command = String.format("curl -X POST -u %s:%s '%s' -d '%s'",user, password, url, params);
					System.out.println(command);
					PrintWriter writer = new PrintWriter("/tmp/runCurlCommand.sh", "UTF-8");
					writer.println(command);
					writer.close(); 
					runCommand("chmod 777 /tmp/runCurlCommand.sh");
					runCommand("/tmp/runCurlCommand.sh");
					runCommand("rm -f /tmp/runCurlCommand.sh");
			    } catch (Exception e) {
			    	e.printStackTrace();
			    }
		}
		
		public static void main(String[] args){
			ClusterInfo info = new ClusterInfo();
			NodeInfo nodeInfo = new NodeInfo();
			BucketInfo bucketInfo = new BucketInfo();
			info.addBucketInfo(bucketInfo);
			info.addNodeInfo(nodeInfo);
			JSONObject obj = nodeInfo.createJSONObject();
			System.out.println(obj.toJSONString());
			System.out.println(bucketInfo.createJSONObject().toJSONString());
			System.out.println(info.createJSONObject().toJSONString()); 
			
			FileWriter file;
			try {
				 file = new FileWriter("config.json");
				 file.write(info.createJSONObject().toJSONString());
		         System.out.println("Successfully Copied JSON Object to File...");
		         file.close();
		 
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			String path = "config.json";
			try {
				ClusterInfo clusterInfo  = ClusterSetupUtils.readConfigFile(path);
				ClusterSetupUtils.initializeCluster(clusterInfo);
				ClusterSetupUtils.createBuckets(clusterInfo);
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		

}
