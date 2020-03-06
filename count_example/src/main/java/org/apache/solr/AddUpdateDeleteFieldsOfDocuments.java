package org.apache.solr;

import com.lucidworks.spark.util.SolrSupport;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;

public class AddUpdateDeleteFieldsOfDocuments {

    public static void main(String[] args) throws IOException, SolrServerException {

        String zkHost="localhost:9983";
        String collection = "analysts";
        SolrClient solrServer = SolrSupport.getCachedCloudClient(zkHost);
        ((CloudSolrClient) solrServer).setDefaultCollection(collection);
        addDoc(solrServer);
        System.exit(0);

    }

    private static void addDoc(SolrClient solrServer) throws SolrServerException, IOException {
        SolrInputDocument solrInputDocument = new SolrInputDocument();
        solrInputDocument.addField("id", "1");
        solrServer.add(solrInputDocument);
        solrServer.commit();
    }





}
