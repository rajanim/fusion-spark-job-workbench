package com.lucidworks.spark.examples

import com.lucidworks.spark.examples.RemoveDuplicateProfilesFromSolrCollection.logger
import com.lucidworks.spark.util.SolrSupport
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.response.QueryResponse
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

object DeleteUnderPerformingQueryReWriteSuggestions {

  val logger = LoggerFactory.getLogger("Delete under performing query rewrite suggestions")

  def main(args: Array[String]): Unit = {
    val queryRewriteZkHost = "localhost:9983"
    val dataCollectionZkHost = "localhost:9983"
    val queryRewriteCollection = "appname_query_rewrite"
    val dataCollection = "appname"
    val suggestionsFieldName = "output"
    val fieldToQuery = "_text_"
    val suggestedQueries = getSuggestedQueries(queryRewriteZkHost, queryRewriteCollection, suggestionsFieldName)
    val zeroResultQueries = getZeroResultsQueries(dataCollectionZkHost, dataCollection, suggestedQueries, fieldToQuery)
    deleteUnderPerformingQuerySuggests(queryRewriteZkHost, queryRewriteCollection, zeroResultQueries, suggestionsFieldName)
  }

  /**
    * //get  the zkhost, collection name and name of the field that has query rewrite suggestions,
    * //query solr by faceting on the query rewrite suggestion field and get the list of query suggestions
    *
    * @param zkHost
    * @param collection
    * @param field
    * @return
    */
  def getSuggestedQueries(zkHost: String, collection: String, field: String): ListBuffer[String] = {
    val solrQuery = new SolrQuery("*:*")
    solrQuery.setFacet(true)
    solrQuery.addFacetField(field)
    solrQuery.setRows(0)
    solrQuery.setFacetMinCount(2)
    solrQuery.setFacetLimit(-1)
    logger.info(s"Sending query to get the list of queries suggestions ${collection} ... ${solrQuery.toString}")
    val qr: QueryResponse = SolrSupport.getCachedCloudClient(zkHost).query(collection, solrQuery)
    val facets = qr.getFacetField(field)
    val facetFieldValues = facets.getValues.iterator()
    val queries = new ListBuffer[String]()
    while (facetFieldValues.hasNext) {
      queries += facetFieldValues.next().getName
    }
    logger.info("total number of records  and facets: " + qr.getResults.getNumFound + "facets: " + queries.size)

    queries

  }

  /**
    * //for each suggestion, query  the data collection and if numFound=0, mark the string for deletion.
    * @param zkHost
    * @param collection
    * @param suggestedQueries
    * @param fieldToQuery
    * @return
    */
  def getZeroResultsQueries(zkHost: String, collection: String, suggestedQueries: ListBuffer[String],
                            fieldToQuery: String): ListBuffer[String] = {
    val zeroResultQueries = new ListBuffer[String]()
    for (query <- suggestedQueries) {
      val solrQuery = new SolrQuery(query.trim)
      solrQuery.setRows(0)
      logger.info(s"Sending query to get the numFound for the query ${collection} ... ${solrQuery.toString}")
      val qr: QueryResponse = SolrSupport.getCachedCloudClient(zkHost).query(collection, solrQuery)
      if (qr.getResults.getNumFound == 0) {
        zeroResultQueries += query
      }
    }
    zeroResultQueries
  }

  /**
    *  //iterate through the list of zero resulting queries, generate a delete query and delete them
    * @param zkHost
    * @param collection
    * @param zeroResultQueries
    * @param fieldName
    */
  def deleteUnderPerformingQuerySuggests(zkHost: String, collection: String, zeroResultQueries: ListBuffer[String], fieldName: String) = {
    for (suggestion <- zeroResultQueries) {
      val query = fieldName + ":" + suggestion
      SolrSupport.getCachedCloudClient(zkHost).deleteByQuery(collection, query, 1000).getStatus
    }
  }

}
//when running on fusion, uncomment the below line
//DeleteUnderPerformingQueryReWriteSuggestions.main(Array())

