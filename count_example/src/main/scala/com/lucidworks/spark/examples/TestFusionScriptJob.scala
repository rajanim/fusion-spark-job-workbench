package com.lucidworks.spark.examples

import org.slf4j.LoggerFactory

class TestFusionScriptJob {

//logger object
  val logger = LoggerFactory.getLogger("test script")

//add method
  def testAdd(num1: Int, num2: Int): Int = {
    return num1 + num2
  }

  //use logger statements
  logger.info("testing addition")

  //invoke method
  val sum = testAdd(2,3)

  //log it and print it as output
  logger.info(s"""sum = $sum""")
  println(sum)
}


object TestFusionScriptJob{

  def main(args: Array[String]): Unit = {
    val testFusionScriptJob = new TestFusionScriptJob
    val add = testFusionScriptJob.testAdd(2,3)

    println("add o/p", add)
  }
}

//for fusion scala script
//TestFusionScriptJob.main(Array())
/*

val productMap = Map(
"collection" -> "lem",
"query" -> "*:*",
"fields" -> "id,unified_prod_id:product.unifiedId_s",
"flatten_multivalued" -> "false")
val productData = spark.read.format("solr").options(productMap).load
val signalsMap = Map(
"collection" -> "Signal_Analysis",
"query" -> "*:*",
"fields" -> "id,Products,Unified_ID",
"flatten_multivalued" -> "false")
val signalData = spark.read.format("solr").options(signalsMap).load
val joined = signalData.select("id", "Products",  "Unified_ID").
withColumnRenamed("id", "signal_id_s").
join(productData, productData.col("unified_prod_id") === signalData.col("Unified_ID"))
joined.write.format("solr").options(Map("collection" -> "lu_joined_signals", "commit_within" -> "5000")).save*/
