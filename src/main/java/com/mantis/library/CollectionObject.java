package com.mantis.library;

import java.io.Serializable;

public class CollectionObject implements Serializable{

	private static final long serialVersionUID = 5039331972643374878L;

	private String collectionName;
	private String zkConfigName;
	private int numShards; 
	private int numReplicas;
	private int maxShardsPerNode;
	
	public CollectionObject(String collectionName, String zkConfigName, int numShards, int numReplicas,
			int maxShardsPerNode) {
		super();
		this.collectionName = collectionName;
		this.zkConfigName = zkConfigName;
		this.numShards = numShards;
		this.numReplicas = numReplicas;
		this.maxShardsPerNode = maxShardsPerNode;
	}

	public CollectionObject() {
	}
	
	public String getCollectionName() {
		return collectionName;
	}
	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}
	public String getZkConfigName() {
		return zkConfigName;
	}
	public void setZkConfigName(String zkConfigName) {
		this.zkConfigName = zkConfigName;
	}
	public int getNumShards() {
		return numShards;
	}
	public void setNumShards(int numShards) {
		this.numShards = numShards;
	}
	public int getNumReplicas() {
		return numReplicas;
	}
	public void setNumReplicas(int numReplicas) {
		this.numReplicas = numReplicas;
	}
	public int getMaxShardsPerNode() {
		return maxShardsPerNode;
	}
	public void setMaxShardsPerNode(int maxShardsPerNode) {
		this.maxShardsPerNode = maxShardsPerNode;
	}
	
	
}
