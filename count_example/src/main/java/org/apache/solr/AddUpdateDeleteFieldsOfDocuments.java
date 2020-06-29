package org.apache.solr;

import com.lucidworks.apollo.component.SolrClientFactory;
import com.lucidworks.spark.util.SolrSupport;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.http.client.methods.HttpPost;
import org.apache.spark.ml.param.Params;


import java.io.IOException;

public class AddUpdateDeleteFieldsOfDocuments {

    public static void main(String[] args) throws IOException, SolrServerException {

        String zkHost="localhost:9983";
        String collection = "analysts";
        SolrClient solrServer = SolrSupport.getCachedCloudClient(zkHost);
        ((CloudSolrClient) solrServer).setDefaultCollection(collection);
        addDoc(solrServer);
        System.exit(0);

        SolrQuery query = new SolrQuery();
        SolrClient client = new HttpSolrClient()
    }

    private static void addDoc(SolrClient solrServer) throws SolrServerException, IOException {
        SolrInputDocument solrInputDocument = new SolrInputDocument();
        solrInputDocument.addField("id", "1");
        solrServer.add(solrInputDocument);
        solrServer.commit();
    }





}
