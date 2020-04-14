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
    if(reports!=null && reports.getResults!=null && reports.getResults.getNumFound>0) {
      logger.info("reports docs", reports.getResults.getNumFound)

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
        if (entitiesQueryResponse != null && entitiesQueryResponse.getResults!=null && entitiesQueryResponse.getResults.getNumFound>0)
          logger.info("entitiesQueryResponse: ", entitiesQueryResponse.getResults.getNumFound)
        val linkedEntitiesIter = entitiesQueryResponse.getResults.iterator()
        while (linkedEntitiesIter.hasNext) {
          val linkedEntity = linkedEntitiesIter.next()
          val linkObj = getLinkedObject(report, linkedEntity)
          solrInputDocs.add(linkObj)
          if (solrInputDocs.size() > batchSize) {
            SolrSupport.sendBatchToSolr(solrClient, destinationCollection, JavaConversions.collectionAsScalaIterable(solrInputDocs), Option(1000))
            solrInputDocs.clear()
          }

        }

      }
      SolrSupport.sendBatchToSolr(solrClient, destinationCollection, JavaConversions.collectionAsScalaIterable(solrInputDocs), Option(1000))
    }
  }

  def getLinkedObject(fromDoc: SolrDocument, toDoc: SolrDocument): SolrInputDocument = {
    val solrInputDocument = new SolrInputDocument()
    if(fromDoc.containsKey("id"))
    solrInputDocument.addField("from_id_s", fromDoc.get("id").toString)

    if(fromDoc.containsKey("common_main_s"))
    solrInputDocument.addField("from_name_s", fromDoc.get("common_main_s"))

    if(fromDoc.containsKey("common_component_type_s"))
    solrInputDocument.addField("from_type_s", fromDoc.get("common_component_type_s"))

    if(fromDoc.containsKey("id"))
    solrInputDocument.addField("to_id_s", toDoc.get("id").toString)

    if(fromDoc.containsKey("common_main_s"))
    solrInputDocument.addField("to_name_s", toDoc.get("common_main_s").toString)

    if(fromDoc.containsKey("common_component_type_s"))
    solrInputDocument.addField("to_type_s", toDoc.get("common_component_type_s"))

    if(fromDoc.containsKey("rsrch_create_datetime_dt"))
      solrInputDocument.addField("validBeginDate_tdt", fromDoc.get("rsrch_create_datetime_dt"))

    if(fromDoc.containsKey("rsrch_product_expiry_datetime_dt"))
      solrInputDocument.addField("validEndDate_tdt", fromDoc.get("rsrch_product_expiry_datetime_dt"))//"9999-12-31T00:00:00.000Z")
    if(fromDoc.containsKey("id"))
      solrInputDocument.addField("linkStrength_d", "1")
    if(fromDoc.containsKey("common_component_type_s"))
      solrInputDocument.addField("link_name", fromDoc.get("common_component_type_s") + "_" + toDoc.get("common_component_type_s"))
    solrInputDocument


  }

  def getConnectedEntitiesQry(document: SolrDocument): String = {
    val fields = document.iterator()
    val stringBuffer = new StringBuffer()
    while (fields.hasNext) {
      val field = fields.next
      val fieldName = field.getKey
      val fieldValue = field.getValue
      fieldName match {
        case "rsrch_primary_company_id_ss" => {
          val values = fieldValue.asInstanceOf[java.util.List[String]].iterator()
          val spaceSeparatedValues = new StringBuffer()
          while (values.hasNext)
            spaceSeparatedValues.append(values.next()).append(" ")
          stringBuffer.append("rsrch_primary_company_id_ss:(").append(spaceSeparatedValues.toString.trim).append(")").append(" OR ")
        }
        case "rsrch_secondary_company_id_ss" => {
          val values = fieldValue.asInstanceOf[java.util.List[String]].iterator()
          val spaceSeparatedValues = new StringBuffer()
          while (values.hasNext)
            spaceSeparatedValues.append(values.next()).append(" ")
          stringBuffer.append("rsrch_secondary_company_id_ss:(").append(spaceSeparatedValues.toString.trim).append(")").append(" OR ")
        }
        case _ => {println(fieldName)}
      }
    }

    if (stringBuffer.toString.endsWith(" OR ")) {
      val strLength = stringBuffer.length
      stringBuffer.delete(strLength - 4, strLength)
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