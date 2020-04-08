package com.lucidworks.spark.examples

import java.util

import com.lucidworks.spark.util.{SolrQuerySupport, SolrSupport}
import com.lucidworks.spark.util.SolrSupport.getCachedCloudClient
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.{SolrDocument, SolrInputDocument}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions



object LinkObjsGenerator {
  val logger = LoggerFactory.getLogger("Fusion Link Objects Generator")


  def main(args: Array[String]): Unit = {
    val zkHost = System.getProperty("solr.zkhost")
    logger.info("zkhost", zkHost);
    val sc = SparkContext.getOrCreate()
    val sourceCollection = sc.getConf.get("spark.sourceCollection")
    logger.info("sourceCollection", sourceCollection);
    val destinationCollection = sc.getConf.get("spark.destinationCollection") //"JPM_Search_2_Fusion_KG_Links"
    logger.info("destinationCollection", destinationCollection);
    val batchSize = sc.getConf.get("spark.solrBatchSize").toInt
    //get solr client
    val solrClient = getCachedCloudClient(zkHost)
    //query documents for category/entities type
    val reports = getEntitiesDocuments(zkHost, sourceCollection, "common_component_type_s", "Report")
    if(reports!=null){
      logger.info("reports docs", reports.getResults.getNumFound)
    }
    //iterator through docs
    val reportsIter = reports.getResults.iterator()
    //add newly created docs to list
    val solrInputDocs = new util.LinkedList[SolrInputDocument]
    //iterate through reports
    while (reportsIter.hasNext) {
      val report = reportsIter.next()
      val entitiesQuery = getConnectedEntitiesQry(report)
      logger.info("entitiesQuery: ", entitiesQuery)
      val solrQuery = new SolrQuery().setQuery(entitiesQuery)
      val entitiesQueryResponse = querySolr(zkHost, sourceCollection, solrQuery)
      if(entitiesQueryResponse!=null)
      logger.info("entitiesQueryResponse: ", entitiesQueryResponse.getResults.getNumFound)
      val linkedEntitiesIter = entitiesQueryResponse.getResults.iterator()
      while (linkedEntitiesIter.hasNext) {
        val linkedEntity = linkedEntitiesIter.next()
        val linkObj = getLinkedObject(report, linkedEntity)
        solrInputDocs.add(linkObj)
        if(solrInputDocs.size()>batchSize){
          SolrSupport.sendBatchToSolr(solrClient, destinationCollection, JavaConversions.collectionAsScalaIterable(solrInputDocs), Option(1000))
        solrInputDocs.clear()
        }

      }

    }
    SolrSupport.sendBatchToSolr(solrClient, destinationCollection, JavaConversions.collectionAsScalaIterable(solrInputDocs), Option(1000))

  }

  def getLinkedObject(fromDoc: SolrDocument, toDoc: SolrDocument): SolrInputDocument = {
    val solrInputDocument = new SolrInputDocument()
    solrInputDocument.addField("from_id_s", fromDoc.get("id").toString)
    solrInputDocument.addField("from_name_s", fromDoc.get("common_main_s"))
    solrInputDocument.addField("from_type_s", fromDoc.get("common_component_type_s"))
    solrInputDocument.addField("to_id_s", toDoc.get("id").toString)
    solrInputDocument.addField("to_name_s", "common_main_s")
    solrInputDocument.addField("to_type_s", toDoc.get("common_component_type_s"))
    //todo - update field value with right field name - validBeginDate_tdt
    solrInputDocument.addField("validBeginDate_tdt", toDoc.get("report_date_tdt"))
    solrInputDocument.addField("validEndDate_tdt", "9999-12-31T00:00:00.000Z")
    solrInputDocument.addField("linkStrength_d", "1")
    solrInputDocument.addField("link_name", fromDoc.get("common_component_type_s") + "_" + toDoc.get("common_component_type_s"))

    solrInputDocument

  }

  def getConnectedEntitiesQry(document: SolrDocument): String = {
    val fields = document.iterator()
    val stringBuffer = new StringBuffer()
    while (fields.hasNext) {
      val field = fields.next
      val fieldName = field.getKey
      val fieldValue = field.getValue.toString
      fieldName match {
        case "rsrch_primary_company_id_ss" => {
          stringBuffer.append("rsrch_primary_company_id_ss:").append(fieldValue).append(" OR ")

        }
        case "rsrch_secondary_company_id_ss" => {
          stringBuffer.append("rsrch_secondary_company_id_ss:").append(fieldValue).append(" OR ")
        }
      }
    }

    if (stringBuffer.toString.endsWith(" OR ")) {
      val strLength = stringBuffer.length - 1
      stringBuffer.delete(strLength - 3, strLength)
    }

    stringBuffer.toString
  }

  def getEntitiesDocuments(zkHost: String, collection: String, entityFieldName: String, entity: String): QueryResponse = {
    val solrQuery = new SolrQuery()
    solrQuery.setQuery("*:*");
    solrQuery.addFilterQuery(entityFieldName + ":" + entity.trim)
    val queryResponse = querySolr(zkHost, collection, solrQuery)
    queryResponse
  }


  //generic methods

  def querySolr(zkHost: String, collection: String, solrQuery: SolrQuery): QueryResponse = {
    val solrClient = getCachedCloudClient(zkHost)
    solrQuery.set("collection", collection)
    SolrQuerySupport.querySolr(solrClient, solrQuery, 0, null).get
  }

  def getSolrInputDoc(input: util.Map[String, String]): SolrInputDocument = {
    val iter = input.entrySet().iterator()
    val solrInputDocument: SolrInputDocument = new SolrInputDocument()
    while (iter hasNext) {
      val keyValue = iter.next()
      solrInputDocument.addField(keyValue.getKey.toString, keyValue.getValue.toString)
    }
    solrInputDocument

  }

  def querySolr(zkHost: String, collection: String, searchQuery: String, fields: String): QueryResponse = {
    val solrClient = getCachedCloudClient(zkHost)
    val solrQuery = new SolrQuery(searchQuery)
    solrQuery.set("collection", collection)
    solrQuery.setFields(fields)
    SolrQuerySupport.querySolr(solrClient, solrQuery, 0, null).get
  }

}

//todo :  uncomment when running this script as a scala spark shell job in fusion.
//LinkObjsGenerator.main(Array())