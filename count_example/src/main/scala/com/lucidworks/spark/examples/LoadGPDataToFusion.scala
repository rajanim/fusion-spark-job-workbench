/*
package com.lucidworks.spark.examples


import com.lucidworks.spark.fusion.FusionPipelineClient
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import java.util.UUID.randomUUID

object LoadGPDataToFusion extends java.io.Serializable {
  val sc: org.apache.spark.SparkContext = ???
  val spark: org.apache.spark.sql.SparkSession = ???
  val fusionEndpoints = "http://10.0.0.8:8765"
  val pipelinePath = "/api/v1/index-pipelines/AllProducts/collections/AllProducts/index"
  val fusionBatchSize = 1000
  ​

  def main(args: Array[String]) {
    // convert the text file into records using a custom record delimiter REC
    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "REC")
    val products = sc.textFile("/opt/lw/gpdata/e_appprod7.txt").map(s => {
      val rec = new scala.collection.mutable.ListBuffer[(String, String)]()
      val cols = s.trim.split("\n").foreach(p => {
        if (p.split('|').length > 1) {
          rec += ((p.split('|')(0), p.split('|')(1)))
        }
        //p.split("\\|") match {
        //case Array(k,v) => rec += ((k,v))
        //case Array(k,v) => println("name: " + k + " value: " + v)
        //}
      })
      rec
    }).filter(!_.isEmpty).repartition(1000)
    ​
    // after partitioning the RDD, send the records to the pipeline as PipelineDocuments
    products.foreachPartition(rows => {
      val fusion: FusionPipelineClient = new FusionPipelineClient(fusionEndpoints)
      val batch = new ListBuffer[Map[String, _]]()
      // convert each row in the partition into a PipelineDocument and add to the batch
      rows.foreach(r => {
        val fields = new ListBuffer[Map[String, _]]();
        r.foreach(t => fields += Map("name" -> t._1, "value" -> t._2))
        batch += Map("id" -> randomUUID().toString, "fields" -> fields)
      })
      if (!batch.isEmpty) {
        println(">> batch has: " + batch.size)
        fusion.postBatchToPipeline(pipelinePath, bufferAsJavaList(batch))
        batch.clear()
      }
    })
  }
}

//LoadGPDatatoFusion.main(Array())
//System.exit(0)
*/
