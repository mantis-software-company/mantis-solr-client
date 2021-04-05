package com.mantis.library;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;

public class SolrRepository implements Closeable,Serializable{

	private static final long serialVersionUID = 5437397126222502752L;

	public enum Mode {
		CLOUD, HTTP
	}

	private SolrClient solrClient;
	private String solrUrl;

	public SolrRepository(Mode mode, String solrUrl) {
		this.solrUrl = solrUrl;
		createConnection(mode);

	}

	private void createConnection(Mode mode) {
		if (mode == Mode.CLOUD) {
			List<String> list = Arrays.asList(this.solrUrl.split(","));
			Optional<String> optional = Optional.empty();
			solrClient = new CloudSolrClient.Builder(list,optional).build();
		} else {
			solrClient = new HttpSolrClient.Builder(this.solrUrl).build();
		}

	}

	public void close() throws IOException {
		try {
			solrClient.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public List<String> getCollectionList() {
		List<String> collectionsList = null;
		try {
			collectionsList = CollectionAdminRequest.List.listCollections(solrClient);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (SolrServerException e) {
			throw new RuntimeException(e);
		}
		return collectionsList;
	}

	public boolean createCollection(CollectionObject collectionObject) {
		// final String solrZKConfigName = "_default";
		final String solrZKConfigName = collectionObject.getZkConfigName();
		final CollectionAdminRequest.Create adminRequest = CollectionAdminRequest.Create
				.createCollection(collectionObject.getCollectionName(), solrZKConfigName, collectionObject.getNumShards(), collectionObject.getNumReplicas())
				.setMaxShardsPerNode(collectionObject.getMaxShardsPerNode());
		CollectionAdminResponse adminResponse = null;
		try {
			adminResponse = adminRequest.process(solrClient);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		// return "true" if collection have been created successfully
		return adminResponse.isSuccess();
	}

	public boolean deleteCollection(String collectionName) {
		final CollectionAdminRequest.Delete adminRequest = CollectionAdminRequest.Delete
				.deleteCollection(collectionName);
		CollectionAdminResponse adminResponse = null;
		try {
			adminResponse = adminRequest.process(solrClient);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		// return "true" if collection have been deleted successfully
		return adminResponse.isSuccess();
	}

	public boolean clearCollection(String collectionName) {
		try {
			solrClient.deleteByQuery(collectionName,"*:*");
			solrClient.commit(collectionName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		return true;
	}

	public boolean reloadCollection(String collectionName) {
		final CollectionAdminRequest.Reload adminRequest = CollectionAdminRequest.Reload
				.reloadCollection(collectionName);
		CollectionAdminResponse adminResponse = null;
		try {
			adminResponse = adminRequest.process(solrClient);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return adminResponse.isSuccess();
	}

	public String getClusterStatus() {
		final CollectionAdminRequest.ClusterStatus adminRequest = CollectionAdminRequest.ClusterStatus
				.getClusterStatus();
		CollectionAdminResponse adminResponse = null;
		try {
			adminResponse = adminRequest.process(solrClient);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return adminResponse.toString();
	}

	public void index(List<SolrInputDocument> records, String collectionName) {
		try {
			for (SolrInputDocument doc : records) {
				solrClient.add(collectionName, doc);
			}
			solrClient.commit(collectionName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

    public QueryResponse createResponse(SolrQuery solrQuery,String collectionName) {
		QueryResponse solrDocs = null;
		try {
			QueryResponse queryResponse = solrClient.query(collectionName,solrQuery);
			solrDocs = queryResponse;
		} catch (Exception e ) {
			throw new RuntimeException(e);
		}
		return solrDocs;
	}


	public SolrDocumentList search(SolrQuery solrQuery,String collectionName) {
		SolrDocumentList solrDocs = null;
		try {
			QueryResponse queryResponse = solrClient.query(collectionName,solrQuery);
			solrDocs = queryResponse.getResults();
		} catch (Exception e ) {
			throw new RuntimeException(e);
		}
		return solrDocs;
	}
	
	/**
	 * 
	 * @param solrQuery
	 * @param collectionName
	 * @return
	 */
	public QueryResponse searchFullResponse(SolrQuery solrQuery,String collectionName) {
		QueryResponse result = null;
		try {
			result = solrClient.query(collectionName,solrQuery);
		} catch (Exception e ) {
			throw new RuntimeException(e);
		}
		return result;
	}
	
	public QueryResponse searchFullResponse(MapSolrParams solrQuery,String collectionName) {
		QueryResponse result = null;
		try {
			result = solrClient.query(collectionName,solrQuery);
		} catch (Exception e ) {
			throw new RuntimeException(e);
		}
		return result;
	}
	
	public SolrDocumentList search(MapSolrParams solrQuery,String collectionName) {
		SolrDocumentList solrDocs = null;
		try {
			QueryResponse queryResponse = solrClient.query(collectionName,solrQuery);
			solrDocs = queryResponse.getResults();
		} catch (Exception e ) {
			throw new RuntimeException(e);
		}
		return solrDocs;
	}
	
	public boolean removeRecordByQuery(String collectionName,String query) {
		try {
			solrClient.deleteByQuery(collectionName,query);
			solrClient.commit(collectionName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		return true;
	}
}
