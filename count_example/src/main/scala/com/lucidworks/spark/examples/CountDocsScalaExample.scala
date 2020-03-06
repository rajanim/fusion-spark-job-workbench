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
      "collection" -> "analysts",
      "query" -> "*:*",
      "fl" -> "id",
    "zkhost"-> "localhost:9983")
    val data = spark.read.format("solr").options(opts).load

    logger.info("Document count is  " + data.count())
    println(data.count())
    spark.stop()
    System.exit(0)
  }

}
