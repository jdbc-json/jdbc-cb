package com.couchbase.jdbc;

import java.util.LinkedList;

public class TestResultAnalysis {
	public int totalQueries =0;
	public int totalPassResults = 0;
	public double passPercentage = 0;
	public LinkedList<String> failureResults = new LinkedList<String>();
	
	public TestResultAnalysis(){
		
	}
	public void addFailureResult(String data){
		this.failureResults.add(data);
	}
	
	public boolean isTestPassing(){
		if(this.totalQueries == this.totalPassResults){
			return true;
		}
		return false;
	}
	
	public void publishResult(){
		System.out.println("\n ++++++++++++++++++++++ START RESULT +++++++++++++++++++++++");
		this.publishFailureResult();
		System.out.printf("\n total queries run %d", totalQueries);
		System.out.printf("\n Pass  %d", totalPassResults);
		System.out.println("\n ++++++++++++++++++++++ END RESULT +++++++++++++++++++++++");
	}
	
	public void publishFailureResult(){
		for(String data:failureResults){
			System.out.println("_________________________________________________________________");
			System.out.println(data);
		}
	}

}
