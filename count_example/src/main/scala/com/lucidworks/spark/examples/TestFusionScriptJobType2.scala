package com.lucidworks.spark.examples

object TestFusionScriptJobType2 {

  def main(args: Array[String]): Unit = {
    printFromMethod()

    println(add(2,3))
  }

  def printFromMethod() ={
    println("Test script")
  }

  def add(a : Int, b:Int) : Int ={

    return (a+b)
  }

}

/*object TestFusionScriptJobType2 {

  /*def main(args: Array[String]): Unit = {
    printFromMethod()

    println(add(2,3))
  }*/

  def printFromMethod() ={
    println("Test script")
    println(add(2,3))
  }

  def add(a : Int, b:Int) : Int ={

    return (a+b)
  }

}

//for fusion scala script
TestFusionScriptJobType2.printFromMethod()*/

//for fusion scala script - it needs a main
//TestFusionScriptJobType2.main(Array())


// the below also works

/*object TestFusionScriptJobType2 extends App {

  /*def main(args: Array[String]): Unit = {
    printFromMethod()

    println(add(2,3))
  }*/

  def printFromMethod() ={
    println("Test script")
    println(add(2,3))
  }

  def add(a : Int, b:Int) : Int ={

    return (a+b)
  }

}*/

//for fusion scala script
//TestFusionScriptJobType2.printFromMethod()