package com.dmac.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class SolrServer {

	public static void main(String[] args) {
		
		SolrServer solrServer = new SolrServer();
		solrServer.queryOnSolr();
		
		
		//solrServer.writeDocumentToSolr();
	}

	
	public void writeDocumentToSolr() {
		
		HttpSolrClient client = new HttpSolrClient("http://localhost:8983/solr/uidai-demographic");
				
		
		SolrInputDocument input = new SolrInputDocument();
		input.addField( "uid", "764567345689");
		input.addField( "address", "rnpuram");
		input.addField( "state", "tamil nadu" );
	    
	    Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
	    docs.add(input);
	    
		try {
			client.add(docs);
			client.commit();
			client.close();
		} catch (SolrServerException | IOException e) {
			System.out.println(e.getMessage());
		}
		
	}
	
	public void queryOnSolr() {
		
		HttpSolrClient client = new HttpSolrClient("http://localhost:8983/solr/uidai-demographic");
		
		SolrQuery query = new SolrQuery();
		    query.setQuery("uid=764567345689");
		
		    query.setFields("uid","address","state");
		    query.setStart(0);    
		    query.set("defType", "edismax");
		try {
			QueryResponse response = client.query(query);
			SolrDocumentList results = response.getResults();
			
			System.out.println(results.size());
			for (int i = 0; i < results.size(); ++i) {
			      System.out.println(results.get(i).getFieldValue("state"));
			}
			
			client.close();
		} catch (SolrServerException | IOException e) {
			e.printStackTrace();
		}
		 
		 
	}
}
