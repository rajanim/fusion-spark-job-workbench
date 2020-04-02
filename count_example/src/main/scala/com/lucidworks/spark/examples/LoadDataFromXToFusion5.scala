/*
package com.lucidworks.spark.examples

class LoadDataFromXToFusion {

}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import java.util.regex.Pattern
import java.net.URI
import org.apache.spark.rdd.RDD
import org.springframework.web.client.RestTemplate
import org.springframework.http.RequestEntity
import org.springframework.http.ResponseEntity
import org.springframework.core.env.Environment
import org.springframework.http._
import com.lucidworks.apollo.common.pipeline._
import com.lucidworks.client.api.IndexPipelineClient
import com.lucidworks.client.models.PipelineDocuments
import com.lucidworks.dc.util.FeignClientUtil
import com.lucidworks.dc.schema.SpringContext
import com.lucidworks.spark.fusion.FusionPipelineClient
import com.lucidworks.cloud.security.ServiceAccountJwtSupport
object LoadEndecaDataToFusion extends java.io.Serializable {
  val productIdField = "P_PRODUCT_ID"
  val productsFile = sc.getConf.get("spark.endeca.products.file")
  val splitToken = sc.getConf.get("spark.endeca.input.split.token")
  val recordDelimiter = sc.getConf.get("spark.endeca.input.recordDelimiter")
  val indexPipeline = sc.getConf.get("spark.endeca.output.fusion.indexPipeline")
  val collection = sc.getConf.get("spark.endeca.output.fusion.collection")
  def main(args:Array[String]) {
    val products = fileToRDD(productsFile)
    sendRDDToFusion(products,"product",productsFile)
  }
  def fileToRDD(filePath:String):RDD[(String,ListBuffer[(String,String)])] = {
    println("************* fileToRDD function called on: "+ filePath)
    sc.hadoopConfiguration.set("textinputformat.record.delimiter",recordDelimiter)
    val records = sc.textFile(filePath).filter(s => s != null && s != "").map(s => {
      var id = new String()
      val fields = new scala.collection.mutable.ListBuffer[(String,String)]()
      //Add input file as field on each record
      val cols = s.trim.split("\n").filter(col => col != null && col != "").foreach(p => {
        val parts = p.split(splitToken,2)
        if (parts.length > 1) {
          val index0 = parts(0)
          val index1 = parts(1)
          //Set ID for record pair
          if(index0 == productIdField){
            id = index1
          }
          if (parts.length > 2) {
            val index2 = parts(2)
            val fieldName = index0 + "_" + index1
            fields += ((fieldName, index2))
          }
          if (parts(1).trim != "") {
            fields += ((index0, index1))
          }
        }
      })
      val rec = (id,fields)
      rec
    }).filter((r) => !r._2.isEmpty).repartition(500)
    return records
  }
  def sendRDDToFusion(records:RDD[(String,ListBuffer[(String,String)])],recordType:String,inputFilePath:String):Unit = {
    println("************* sendRDDToFusion function called on: "+ records + ", " + recordType + ", " + inputFilePath)
    records.foreachPartition(rows => {
      val feignClientUtil = new FeignClientUtil
      val indexPipelineClient = feignClientUtil.getFeignClient(classOf[IndexPipelineClient], "indexing")
      val batch = new ListBuffer[PipelineDocument]()
      val batchSize = 1000
      // convert each row in the partition into a PipelineDocument and add to the batch
      rows.foreach(r => {
        println("************* processing row: "+ r)
        val fields = new ListBuffer[PipelineField]();
        fields += new PipelineField("type_str", recordType)
        fields += new PipelineField("input_file_string", inputFilePath)
        r._2.foreach(t => {
          fields += new PipelineField((t._1 + "_t"), t._2)
          fields += new PipelineField((t._1 + "_ss"), t._2)
        })
        val doc = new PipelineDocument(r._1)
        doc.addFields(bufferAsJavaList(fields))
        batch += doc
        if (batch.size >= batchSize) {
          val docs = new PipelineDocuments(bufferAsJavaList(batch))
          indexPipelineClient.indexPipelineDocument(indexPipeline,collection, false, false, null, false, true, true, null, null, null, docs)
          batch.clear()
        }
      })
      if (!batch.isEmpty) {
        println(">> final batch has: "+batch.size)
        val docs = new PipelineDocuments(bufferAsJavaList(batch))
        indexPipelineClient.indexPipelineDocument(indexPipeline,collection, false, false, null, false, true, true, null, null, null, docs)
        batch.clear()
      }
    })
  }
}
LoadEndecaDataToFusion.main(Array())


import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import java.util.regex.Pattern
import java.net.URI
import org.apache.spark.rdd.RDD
import org.springframework.web.client.RestTemplate
import org.springframework.http.RequestEntity
import org.springframework.http.ResponseEntity
import org.springframework.core.env.Environment
import org.springframework.http._
import com.lucidworks.apollo.common.pipeline._
import com.lucidworks.client.api.IndexPipelineClient
import com.lucidworks.client.models.PipelineDocuments
import com.lucidworks.dc.util.FeignClientUtil
import com.lucidworks.dc.schema.SpringContext
import com.lucidworks.spark.fusion.FusionPipelineClient
import com.lucidworks.cloud.security.ServiceAccountJwtSupport
val productIdField = "P_PRODUCT_ID"
val productsFile = sc.getConf.get("spark.endeca.products.file")
val splitToken = sc.getConf.get("spark.endeca.input.split.token")
val recordDelimiter = sc.getConf.get("spark.endeca.input.recordDelimiter")
val indexPipeline = sc.getConf.get("spark.endeca.output.fusion.indexPipeline")
val collection = sc.getConf.get("spark.endeca.output.fusion.collection")
def fileToRDD(filePath:String):RDD[(String,ListBuffer[(String,String)])] = {
  println("************* fileToRDD function called on: "+ filePath)
  sc.hadoopConfiguration.set("textinputformat.record.delimiter",recordDelimiter)
  val records = sc.textFile(filePath).filter(s => s != null && s != "").map(s => {
    var id = new String()
    val fields = new scala.collection.mutable.ListBuffer[(String,String)]()
    //Add input file as field on each record
    val cols = s.trim.split("\n").filter(col => col != null && col != "").foreach(p => {
      val parts = p.split(splitToken,2)
      if (parts.length > 1) {
        val index0 = parts(0)
        val index1 = parts(1)
        //Set ID for record pair
        if(index0 == productIdField){
          id = index1
        }
        if (parts.length > 2) {
          val index2 = parts(2)
          val fieldName = index0 + "_" + index1
          fields += ((fieldName, index2))
        }
        if (parts(1).trim != "") {
          fields += ((index0, index1))
        }
      }
    })
    val rec = (id,fields)
    rec
  }).filter((r) => !r._2.isEmpty).repartition(500)
  return records
}
val inputFilePath = productsFile
val recordType = "product"
val records = fileToRDD(productsFile)
println("************* sendRDDToFusion function called on: "+ records + ", " + recordType + ", " + inputFilePath)
records.foreachPartition(rows => {
  val feignClientUtil = new FeignClientUtil
  val indexPipelineClient = feignClientUtil.getFeignClient(classOf[IndexPipelineClient], "indexing")
  val batch = new ListBuffer[PipelineDocument]()
  val batchSize = 1000
  // convert each row in the partition into a PipelineDocument and add to the batch
  rows.foreach(r => {
    println("************* processing row: "+ r)
    val fields = new ListBuffer[PipelineField]();
    fields += new PipelineField("type_str", recordType)
    fields += new PipelineField("input_file_string", inputFilePath)
    r._2.foreach(t => {
      fields += new PipelineField((t._1 + "_t"), t._2)
      fields += new PipelineField((t._1 + "_ss"), t._2)
    })
    val doc = new PipelineDocument(r._1)
    doc.addFields(bufferAsJavaList(fields))
    batch += doc
    if (batch.size >= batchSize) {
      val docs = new PipelineDocuments(bufferAsJavaList(batch))
      indexPipelineClient.indexPipelineDocument(indexPipeline,collection, false, false, null, false, true, true, null, null, null, docs)
      batch.clear()
    }
  })
  if (!batch.isEmpty) {
    println(">> final batch has: "+batch.size)
    val docs = new PipelineDocuments(bufferAsJavaList(batch))
    indexPipelineClient.indexPipelineDocument(indexPipeline,collection, false, false, null, false, true, true, null, null, null, docs)
    batch.clear()
  }
})
*/
