package com.lucidworks.spark.examples

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.UUID.randomUUID

import com.lucidworks.apollo.common.pipeline.{PipelineDocument, PipelineField}
import com.lucidworks.spark.examples.GenerateLinks
import com.lucidworks.spark.fusion.FusionPipelineClient
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
import scala.collection.mutable.ListBuffer


class GenerateLinks

object GenerateLinks {

  val logger = LoggerFactory.getLogger(classOf[GenerateLinks])
  val appName = "fusion-generate-link-objects"
  val linksIdField = "_lw_gen_links_id_s"

  //todo move to options
  val fusionEndpoints = "http://10.0.0.8:8765"
  val pipelinePath = "/api/v1/index-pipelines/AllProducts/collections/AllProducts/index"
  val fusionBatchSize = 1000


  def main(args: Array[String]): Unit = {
    val cli = processCommandLineArgs(getOptions, args).get
    val verbose = cli.hasOption("verbose")

    val spark = initSparkSession(cli)
    if (verbose) {
      logger.info(s"Initialized SparkSession using: ${spark.sparkContext.getConf.toDebugString}")
    }

    val destinationSolrClusterZk = cli.getOptionValue("destinationSolrClusterZk")
    val sourceSolrClusterZk = cli.getOptionValue("sourceSolrClusterZk")
    val sourceCollection = cli.getOptionValue("sourceCollection")
    val destinationCollection = cli.getOptionValue("destinationCollection", sourceCollection)
    val sourceQuery = cli.getOptionValue("sourceQuery", "*:*")
    //val timestampFieldName = cli.getOptionValue("timestampField", "timestamp_tdt")
    val batchSize = cli.getOptionValue("batchSize", "10000")
    val findNewOnly = ("true" == cli.getOptionValue("findNewOnly", "true"))
    val optimizeOutput = cli.getOptionValue("optimizeOutput", "0").toInt
    //val useNaturalID = cli.getOptionValue("useNaturalID", "").trim
    val splitsPerShard = cli.getOptionValue("splitsPerShard")
    //val naturalIdFields = if (!useNaturalID.isEmpty) useNaturalID.split(",").toSeq else Seq.empty

    logger.info(s"Fusion Links Generator running with config: sourceSolrClusterZk=${sourceSolrClusterZk}, sourceCollection=${sourceCollection}, destinationSolrClusterZk=${destinationSolrClusterZk}, destinationCollection=${destinationCollection}, sourceQuery=${sourceQuery}, findNewOnly=${findNewOnly}")

    var readFromSourceClusterOpts = Map(
      "zkhost" -> sourceSolrClusterZk,
      "collection" -> sourceCollection,
      "query" -> sourceQuery,
      "flatten_multivalued" -> "false")
    if (splitsPerShard != null) {
      readFromSourceClusterOpts = readFromSourceClusterOpts ++ Map("splits_per_shard" -> splitsPerShard)
    }
    logger.info(s"Reading from source collection using options: ${readFromSourceClusterOpts.mkString(", ")}")
    val sourceCollectionDF = spark.read.format("solr").options(readFromSourceClusterOpts).load
    if (verbose) {
      logger.info(s"Source collection has schema: ${sourceCollectionDF.schema.mkString("; ")}")
    }

    val linksUUID = randomUUID.toString
    logger.info(s"Using ${linksIdField}=${linksUUID} for tracking writes from this transfer process.")

    val rtimer = new RTimer

    sourceCollectionDF.foreachPartition(rows => {
      val fusion: FusionPipelineClient = new FusionPipelineClient(fusionEndpoints)
     // val batch = new ListBuffer[Map[String,_]]()
      val batch = new ListBuffer[PipelineDocument]()
      val batchSize = 5000
      // convert each row in the partition into a PipelineDocument and add to the batch
      rows.foreach(r => {
      //  val fields = new ListBuffer[Map[String,_]]();
      val fields = new ListBuffer[PipelineField]();
        //fields += new PipelineField("name", r.)
        //fields += new PipelineField("age", r.age)
       // val doc = new PipelineDocument(com.lucidworks.dc.util.UUIDUtils.shortRandomId())
        //doc.addFields(bufferAsJavaList(fields))
       // batch += doc

     /*   batch += Map("id" -> randomUUID().toString, "fields" -> fields)
        if (batch.size >= batchSize) {
          fusion.postBatchToPipeline(pipelinePath, bufferAsJavaList(batch))
          batch.clear()
        }*/
      })
      if (!batch.isEmpty) {
        println(">> final batch has: "+batch.size)
        fusion.postBatchToPipeline(pipelinePath, bufferAsJavaList(batch))
        batch.clear()
      }
      fusion.shutdown
    })



  }


  def processCommandLineArgs(appOptions: Array[CliOpt], args: Array[String]): Option[CommandLine] = {
    val options = new Options()
    options.addOption("h", "help", false, "Print this message")
    options.addOption("v", "verbose", false, "Generate verbose log messages")
    appOptions.foreach(options.addOption(_))
    try {
      val cli = (new GnuParser()).parse(options, args);
      if (cli.hasOption("help")) {
        (new HelpFormatter()).printHelp(appName, options)
        System.exit(0)
      }
      return Some(cli)
    } catch {
      case pe: ParseException => {
        if (args.filter(p => p == "-h" || p == "-help").isEmpty) {
          // help not requested ... report the error
          System.err.println("Failed to parse command-line arguments due to: " + pe.getMessage())
        }
        (new HelpFormatter()).printHelp(appName, options)
        System.exit(1)
      }
      case e: Exception => throw e
    }
    None
  }

  def getOptions: Array[CliOpt] = {
    Array(
      CliOpt.builder()
        .hasArg().required(true)
        .desc("ZooKeeper connection string for the Solr cluster this app transfers data to")
        .longOpt("destinationSolrClusterZk").build,
      CliOpt.builder()
        .hasArg().required(true)
        .desc("ZooKeeper connection string for the Solr cluster this app transfers data from")
        .longOpt("sourceSolrClusterZk").build,
      CliOpt.builder()
        .hasArg()
        .desc("Name of the Solr collection on the destination cluster to write data to; uses source name if not provided")
        .longOpt("destinationCollection").build,
      CliOpt.builder()
        .hasArg().required(true)
        .desc("Name of the Solr collection on the source cluster to read data from")
        .longOpt("sourceCollection").build,
      CliOpt.builder()
        .hasArg()
        .desc("Query to source collection for docs to transfer; uses *:* if not provided")
        .longOpt("sourceQuery").build,
      CliOpt.builder()
        .hasArg()
        .desc("Flag to indicate if this app should look for new docs in the source using the latest timestamp in the destination; defaults to true, set to false to skip this check and pull all docs that match the source query")
        .longOpt("findNewOnly").build,
      CliOpt.builder()
        .hasArg()
        .desc("Timestamp field name on docs; defaults to 'timestamp_tdt'")
        .longOpt("timestampField").build,
      CliOpt.builder()
        .hasArg()
        .desc("Batch size for writing docs to the destination cluster; defaults to 10000")
        .longOpt("batchSize").build,
      CliOpt.builder()
        .hasArg()
        .desc("Additional Spark configuration properties file")
        .longOpt("sparkConf").build,
      CliOpt.builder()
        .hasArg()
        .desc("Compute Natural Doc ID by concatenating fields")
        .longOpt("useNaturalID").build,
      CliOpt.builder()
        .hasArg
        .desc("Optimize the destination collection to a configured number of segments. Skips optimization if value is zero")
        .longOpt("optimizeOutput").build,
      CliOpt.builder()
        .hasArg
        .desc("Number of splits per shard while reading from Solr")
        .longOpt("splitsPerShard").build
    )
  }

  def initSparkSession(cli: CommandLine): SparkSession = {
    val sparkConf = new SparkConf()
    if (cli.getOptionValue("sparkConf") != null) {
      val sparkConfPropsFile = new File(cli.getOptionValue("sparkConf"))
      if (!sparkConfPropsFile.isFile) {
        throw new IllegalArgumentException(s"Additional Spark config file ${sparkConfPropsFile.getAbsolutePath} not found!")
      }
      val props = new Properties()
      props.load(new BufferedReader(new InputStreamReader(new FileInputStream(sparkConfPropsFile), "UTF-8")))
      sparkConf.setAll(props.asInstanceOf[java.util.Map[String, String]].toMap)
      logger.info(s"Added custom configuration properties to SparkConf: ${sparkConf.toDebugString}")
    }
    SparkSession.builder().config(sparkConf).appName(appName).getOrCreate()
  }


  //entity country
  //example search - china (tagger detected as entity type - country)
  // get reports, people, companies

  //entity person
  //example search - deepika mundra (tagger detected as Person entity)
  //from : from_id_s to:to_ids ) deepika mundra
  //from id - sid  to

  //
  /*
    "doc_type_s":"link",
    "entity_type_s":"person_company",
    "id":"person_v069131_person_company_company_INGL.NS_20200101T000000000Z",
    "from_entity_type_s":"person",
    "to_entity_type_s":"company",
    "from_id_s":"v069131",
    "to_id_s":"INGL.NS",
    "from_name_s":"Deepika Mundra",
    "to_name_s":"InterGlobe Aviation Ltd",
    "link_begin_date_tdt":"20200101T000000000Z"
  */
  /*
  "doc_type_s":"link",
  "entity_type_s":"person_report",
  "id":"person_v069131_person_report_report_GPS-275757-0_20200101T000000000Z",
  "from_entity_type_s":"person",
  "to_entity_type_s":"company",
  "from_id_s":"v069131",
  "to_id_s":"INGL.NS",
  "from_name_s":"Deepika Mundra",
  "to_name_s":"InterGlobe Aviation Ltd",
  "link_begin_date_tdt":"20200101T000000000Z"
*/


  //
}
