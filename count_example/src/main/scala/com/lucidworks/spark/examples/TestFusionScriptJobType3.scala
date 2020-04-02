package com.lucidworks.spark.examples


import org.slf4j.LoggerFactory

object TestFusionScriptJobType3 {


  //logger object
  val logger = LoggerFactory.getLogger("test script")

  //add method
  def testAdd(num1: Int, num2: Int): Int = {
     num1 + num2
  }



  //use logger statements
  //logger.info("testing addition")

  //invoke method
  //val sum = testAdd(2,3)

  //log it and print it as output
  //logger.info(s"""sum = $sum""")
 // println(sum)

 /* def main(args: Array[String]): Unit = {
    println(testAdd(2,3))
  }*/
}


//for fusion scala script
//val output = TestFusionScriptJobType3.testAdd(9,3)


//println(output)

