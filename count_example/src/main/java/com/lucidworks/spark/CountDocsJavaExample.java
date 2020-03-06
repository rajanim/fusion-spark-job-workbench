package com.lucidworks.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.HashMap;

public class CountDocsJavaExample {

  private static Logger logger = LoggerFactory.getLogger(CountDocsJavaExample.class);

  public static void main(String[] args) {
    SparkSession sparkSession = SparkSession
        .builder()
        .appName("CountDocsJavaExample").config("spark.master", "local")
        .getOrCreate();

      HashMap map = new HashMap<String, String>();
      map.put("zkhost", "localhost:9983");
      map.put("collection", "analysts");


      Dataset dataset = sparkSession.read().format("solr").options(map).load();
    logger.info("No. of docs in collection logs are " + dataset.count());
    System.out.println(dataset.count());
    sparkSession.stop();
    System.exit(0);
  }
}
