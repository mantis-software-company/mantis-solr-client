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

/**
 * Class defines the Solr connection creation with respect of modes,
 * reload,clear, delete, create, get, remove, search Solr Collection by collection name
 * create, remove, index Response by collection name
 */
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
	/**
	 * The method runs for creating connection
	 * @param mode type of a Mode (Cloud or Http)
	 *
	 */
	private void createConnection(Mode mode) {
		if (mode == Mode.CLOUD) {
			List<String> list = Arrays.asList(this.solrUrl.split(","));
			Optional<String> optional = Optional.empty();
			solrClient = new CloudSolrClient.Builder(list,optional).build();
		} else {
			solrClient = new HttpSolrClient.Builder(this.solrUrl).build();
		}

	}
	/**
	 * The method runs for closing the opened connection if any.
	 *
	 */
	public void close() throws IOException {
		try {
			solrClient.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	/**
	 * The method runs for getting collection list
	 * @return type of a collectionsList
	 */
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
	/**
	 * The method runs for creating collection
	 * @param collectionObject includes collection object to create collection
	 * @return type of boolean which gives creating success
	 */
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
	/**
	 * The method runs for delete Collection in the parameter by using CollectionAdminRequest class
	 * @param collectionName includes name of collection as a String
	 * @return type of boolean which gives deleting operation success
	 */

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
	/**
	 * The method runs for delete Collection in the parameter by using query
	 * @param collectionName includes name of collection as a String
	 * @return type of boolean which gives clearing operation success
	 */
	public boolean clearCollection(String collectionName) {
		try {
			solrClient.deleteByQuery(collectionName,"*:*");
			solrClient.commit(collectionName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		return true;
	}
	/**
	 * The method runs for reload collection
	 * @param collectionName includes name of collection as a String
	 * @return type of boolean which gives reload operation success
	 */
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
	/**
	 *
	 * The method runs for getting Cluster Status from SolrClient
	 * @return is adminResponse type of String
	 *
	 */
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
	/**
	 *
	 * The method runs for indexing records.
	 * @param records includes the list of SolrInputDocument
	 * @param collectionName includes the collection name as a String
	 *
	 */
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
	/**
	 *
	 * The method runs for creating response from solr
	 * @param solrQuery includes the solrQuery information
	 * @param collectionName includes the collection name as a String
	 * @return type of QueryResponse
	 *
	 */
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
	/**
	 *
	 * The method runs for searching document list from solr
	 * @param solrQuery includes the solrQuery information
	 * @param collectionName includes the collection name as a String
	 * @return type of SolrDocumentList
	 *
	 */

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
	 * The method runs for searching full response from solr
	 * @param solrQuery includes the solrQuery information
	 * @param collectionName includes the collection name as a String
	 * @return type of QueryResponse
	 *
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
	/**
	 *
	 * The method runs for searching full response from solr
	 * @param solrQuery includes the MapSolrParams information
	 * @param collectionName includes the collection name as a String
	 * @return type of QueryResponse
	 */
	public QueryResponse searchFullResponse(MapSolrParams solrQuery,String collectionName) {
		QueryResponse result = null;
		try {
			result = solrClient.query(collectionName,solrQuery);
		} catch (Exception e ) {
			throw new RuntimeException(e);
		}
		return result;
	}
	/**
	 *
	 * The Method runs for searching document list from solr
	 * @param solrQuery includes the MapSolrParams information
	 * @param collectionName includes the collection name as a String
	 * @return type of SolrDocument
	 */
	public SolrDocumentList search(MapSolrParams solrQuery,String collectionName) {
		SolrDocumentList solrDocs = null;
		try {
			QueryResponse queryResponse = solrClient.query(collectionName,solrQuery);
			solrDocs = queryResponse.getResults();
		} catch (Exception e ) {
			throw new RuntimeException(e);
		}
		return solrDocs;
	}	/**
	 *
	 * The method runs for removing collection using query
	 * @param collectionName includes the collection name as a String
	 * @param query includes the query as a String
	 * @return type of boolean which gives remove operation success
	 *
	 */
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
