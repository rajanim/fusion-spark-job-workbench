package com.lucidworks.spark.examples

import com.lucidworks.spark.util.SolrSupport
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.{SolrDocumentList, SolrInputDocument}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

object RemoveDuplicateProfilesFromSolrCollection {

  val logger = LoggerFactory.getLogger("Remove duplicate profiles from solr collection")


  def removeDuplicateProfiles(zkHost: String, collection: String) = {
    logger.info("Starting at remove duplicate profiles")
    val duplicateProfiles = getDuplicateProfiles(zkHost, collection)
    logger.info("Total number of duplicate profiles: " + duplicateProfiles.size)
    //  val updatedDocsList = new  ListBuffer[SolrInputDocument]
    val batch = ListBuffer.empty[SolrInputDocument]

    for (profile <- duplicateProfiles) {
      val tuple = getProfileIds(zkHost, collection, profile)

      if (tuple != null) {
        for (id <- tuple._2) {
          logger.info("id:" + id)
          val solrDocs = getRecordsToUpdate(zkHost, collection, profile, id)
          if (solrDocs.getNumFound >= 1) {
            val docsIter = solrDocs.iterator()
            while (docsIter.hasNext) {
              val doc = docsIter.next()
              val inputDoc = toSolrInputDocument(doc)
              inputDoc.setField("creator__id", tuple._1)
              batch += inputDoc

            }

          }
          val del = deleteProfileById(zkHost, collection, profile, id)
          logger.info(s"delete status to delete profile id from collection ${collection} ... ${profile}  id is ${id} status ${del}")

          logger.info("delete")
          val solrCLient = SolrSupport.getCachedCloudClient(zkHost)
          SolrSupport.sendBatchToSolr(solrCLient, collection, batch.toList, Option(60))
        }

      } else {
        logger.info("")
      }

    }

    println("Job Execution Completed")

  }

  /**
    * delete solr record - by id
    * @param zkHost
    * @param collection
    * @param profile
    * @param id
    * @return
    */
  def deleteProfileById(zkHost: String, collection: String, profile: String, id: String) : Int = {
    return SolrSupport.getCachedCloudClient(zkHost).deleteById(collection, id, 1000).getStatus
  }


  /**
    * delete solr record - by query
    * @param zkHost
    * @param collection
    * @return
    */
  def deleteProfileByQuery(zkHost: String, collection: String, query:String) : Int = {
    return SolrSupport.getCachedCloudClient(zkHost).deleteByQuery(collection,query,1000).getStatus
  }

  /**
    * update solr document to solr input document
    */
  import org.apache.solr.common.{SolrDocument, SolrInputDocument}

  def toSolrInputDocument(d: SolrDocument): SolrInputDocument = {
    val doc = new SolrInputDocument
    import scala.collection.JavaConversions._
    for (name <- d.getFieldNames) {
      doc.addField(name, d.getFieldValue(name))
    }
    doc.remove("_version_")
    doc
  }

  /**
    * get all the bookmarsk that are under duplicated profile id to merge to main profile id
    * @param zkHost
    * @param collection
    * @param profile
    * @param id
    * @return
    */
  def getRecordsToUpdate(zkHost: String, collection: String, profile: String, id: String): SolrDocumentList = {

    val solrQuery = new SolrQuery(profile + " AND creator__id:" + id)
    //solrQuery.addFilterQuery("NOT(type:profile)")
    solrQuery.setRows(10000)
    logger.info(s"Sending query to get records to update ... ${solrQuery.toString}")
    val qr = SolrSupport.getCachedCloudClient(zkHost).query(collection, solrQuery)
    logger.info("Response found for the above query: " + qr.getResults.getNumFound)
    qr.getResults
  }

  /**
    * get duplicate creator ids
    * @param zkHost
    * @param collection
    * @param profile
    * @return
    */
  def getProfileIds(zkHost: String, collection: String, profile: String): (String, ListBuffer[String]) = {
    val solrQuery = new SolrQuery(profile)
    solrQuery.setFacet(true)
    solrQuery.addFacetField("creator__id")
    solrQuery.setRows(1)
    solrQuery.setFacetMinCount(1)
    logger.info(s"Sending query to get the duplicate profiles creator__id ${collection} ... ${solrQuery.toString}")
    val qr: QueryResponse = SolrSupport.getCachedCloudClient(zkHost).query(collection, solrQuery)
    logger.info("Response found for the above query: " + qr.getResults.getNumFound)
    val creatorIdsFacet = qr.getFacetField("creator__id")
    logger.info("creatorIdsFacet :  " + creatorIdsFacet.getValueCount)
    if (creatorIdsFacet.getValueCount > 1) {
      val mainId = creatorIdsFacet.getValues.get(0).getName
      val duplicateIds = creatorIdsFacet.getValues.iterator()

      val dupList = new ListBuffer[String]()
      duplicateIds.next()
      while (duplicateIds.hasNext) {
        dupList += duplicateIds.next().getName
      }
      new Tuple2(mainId, dupList)
    } else {
      //delete profile records by query  which will filter by type profile and not of main id (reatorIdsFacet.getValues.get(0).getName)
      if(creatorIdsFacet.getValueCount==1) {
        val query = s"${profile} AND NOT(id:${creatorIdsFacet.getValues.get(0).getName}) AND type:profile"
        logger.info("delete query: " + query)
        val del = deleteProfileByQuery(zkHost, collection, query)
        logger.info("delete status: " + del)
      }
      if(creatorIdsFacet.getValueCount==0){
        val id = qr.getResults.get(0).getFieldValue("id")
        val query = s"${profile} AND NOT(id:${id}) AND type:profile"
        logger.info("delete query: " + query)
        val del = deleteProfileByQuery(zkHost, collection, query)
        logger.info("delete status: " + del)
      }

      null
    }

  }

  /**
    * Get the list of duplicate profiles
    * facet=on&facet.field=user_id&facet.mincount=2
    *
    * @param zkHost
    * @param collection
    * @return
    */
  def getDuplicateProfiles(zkHost: String, collection: String): ListBuffer[String] = {
    val solrQuery = new SolrQuery("*:*")
    solrQuery.setFacet(true)
    solrQuery.addFacetField("user_id")
    solrQuery.setRows(0)
    solrQuery.setFacetMinCount(2)
    solrQuery.setFacetLimit(-1)
    logger.info(s"Sending query to get the list of duplicate profiles ${collection} ... ${solrQuery.toString}")
    val qr: QueryResponse = SolrSupport.getCachedCloudClient(zkHost).query(collection, solrQuery)
    val dupProfilesFacets = qr.getFacetField("user_id")
    val facetFieldValues = dupProfilesFacets.getValues.iterator()
    val dupProfilesList = new ListBuffer[String]()
    while (facetFieldValues.hasNext) {
      dupProfilesList += facetFieldValues.next().getName


    }
    logger.info("total number of records  and facets: " + qr.getResults.getNumFound + "facets: " + dupProfilesFacets.getValueCount)

    dupProfilesList
  }

  def main(args: Array[String]): Unit = {

    removeDuplicateProfiles("localhost:9983/lwfusion/4.2.3/solr", "Test3")
  }
}
//RemoveDuplicateProfilesFromSolrCollection.removeDuplicateProfiles("localhost:9983/lwfusion/4.2.3/solr", "Test3")


