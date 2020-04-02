/*
package com.lucidworks.spark.examples



import org.apache.spark
import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.UUID.randomUUID

import org.slf4j.LoggerFactory
import com.lucidworks.spark.util.{SolrQuerySupport, SolrSupport}
import org.apache.commons.cli.{CommandLine, GnuParser, HelpFormatter, Options, ParseException, Option => CliOpt}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.util.RTimer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._


object ExportSolrRecordsToJSON extends App{

  //val spark = initSparkSession(cli)

  val exportRecords = Map(
    "collection" -> "Films",
    "zkhost" -> "localhost:9983/lwfusion/4.2.4/solr",
    "rows" -> "100",
    "fields" -> "*",
    "df" -> "_text_",
    "splits_per_shard" -> "4",
    "solr.params" -> "q=comedy&df=_text_")
  //val exportedRecords = spark.read.format("solr").options(exportRecords).load
  //exportedRecords.count
  //exportedRecords.write.format("json").mode("overwrite").save("/Users/rajanimaski/exportedRecords")



  def initSparkSession(cli: CommandLine) : SparkSession = {
    val sparkConf = new SparkConf()
    if (cli.getOptionValue("sparkConf") != null) {
      val sparkConfPropsFile = new File(cli.getOptionValue("sparkConf"))
      if (!sparkConfPropsFile.isFile) {
        throw new IllegalArgumentException(s"Additional Spark config file ${sparkConfPropsFile.getAbsolutePath} not found!")
      }
      val props = new Properties()
      props.load(new BufferedReader(new InputStreamReader(new FileInputStream(sparkConfPropsFile), "UTF-8")))
      sparkConf.setAll(props.asInstanceOf[java.util.Map[String, String]].toMap)
      //logger.info(s"Added custom configuration properties to SparkConf: ${sparkConf.toDebugString}")
    }
    SparkSession.builder().config(sparkConf).appName(ExportSolrRecordsToJSON).getOrCreate()
  }

}
*/
