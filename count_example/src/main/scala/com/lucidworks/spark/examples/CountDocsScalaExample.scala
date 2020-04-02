package com.lucidworks.spark.examples

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class CountDocsInCollection

object CountDocsInCollection {

  val logger = LoggerFactory.getLogger(classOf[CountDocsInCollection])
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
        .builder
        .appName("CountDocsJavaExample")
        .config("spark.master", "local")
        .getOrCreate()

    val opts = Map(
      "collection" -> "Films_signals_aggr",
      "query" -> "*:*",
      "fl" -> "id",
    "zkhost"-> "localhost:9983/lwfusion/4.2.4/solr")
    val data = spark.read.format("solr").options(opts).load
    logger.info("Document count is : " + data.count())
   // println(data.count())

    data.foreachPartition(rows => {
      rows.foreach({ r => {
       //val doc =  r.getValuesMap(Seq("id", "doc_id_s", "aggr_count_i"))



      }

      })
    })

    logger.info("Document count is  " + data.count())
    println(data.count())
    spark.stop()
    System.exit(0)
  }

}
