package com.lucidworks.spark.examples

import java.util

import com.lucidworks.spark.util.{SolrQuerySupport, SolrSupport}
import com.lucidworks.spark.util.SolrSupport.getCachedCloudClient
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.{SolrDocument, SolrDocumentList, SolrInputDocument}
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions

object ReadWriteToSparkSolr {
  val logger = LoggerFactory.getLogger("Read Write To Spark Solr")


  def main(args: Array[String]): Unit = {
    //cluster props
    val zkHost = System.getProperty("solr.zkhost")
    println("zkhost", zkHost);
    //val zkHost = "localhost:9983/lwfusion/4.2.4/solr"
    val sourceCollection = "Films_signals_aggr"
    val destinationCollection  = "Films"
    val searchQuery = "*:*"
    //get solr client
    val solrClient = getCachedCloudClient(zkHost)

    //read from solr
    val queryResponse = querySolr(zkHost, sourceCollection, searchQuery, "id")
    //get first document
    val doc = queryResponse.getResults.get(0)
    println(doc.getFieldValue("id"))
    //get numFound
    println(queryResponse.getResults.getNumFound)

    //loop through docs list.
    val docs = queryResponse.getResults

    //iterate through docs
    val docsIter = queryResponse.getResults.iterator()
    val solrInputDocs = new util.LinkedList[SolrInputDocument]
    while (docsIter.hasNext) {
      val doc = docsIter.next()
      solrInputDocs.add(SolrSupport.autoMapToSolrInputDoc(doc.get("id").toString, doc, null))
      //println(doc.get("id"))
    }

    //write to solr
    SolrSupport.sendBatchToSolr(solrClient, destinationCollection, JavaConversions.collectionAsScalaIterable(solrInputDocs), Option(1000))


  }

  def querySolr(zkHost: String, collection: String, searchQuery: String, fields: String): QueryResponse = {
    val solrClient = getCachedCloudClient(zkHost)
    val solrQuery = new SolrQuery(searchQuery)
    solrQuery.set("collection", collection)
    solrQuery.setFields(fields)
    SolrQuerySupport.querySolr(solrClient, solrQuery, 0, null).get
  }

  def indexSolr(zkHost: String, collection: String, solrRecords: RDD[SolrInputDocument]) = {
    SolrSupport.indexDocs(zkHost, collection, 100, (solrRecords), Option(1000))
  }


}

//todo :  uncomment when running this script as a scala spark shell job in fusion.
//ReadWriteToSparkSolr.main(Array())
