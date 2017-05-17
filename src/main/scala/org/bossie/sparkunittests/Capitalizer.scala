package org.bossie.sparkunittests

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class Capitalizer(read: String => RDD[String], write: RDD[String] => Unit) {
  def capitalize(textFile: String): Unit = {
    val inputText = read(textFile)
    val capitalized = inputText.map(_.toUpperCase)
    write(capitalized)
  }
}

object Capitalizer {
  def main(args : Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Unit Tests")
    val sc = new SparkContext(conf)

    try {
      val readFromFile = (textFile: String) => sc.textFile(textFile)
      val printToConsole = (outputLines: RDD[String]) => outputLines.collect().foreach(println)

      val capitalizer = new Capitalizer(readFromFile, printToConsole)
      capitalizer.capitalize("/home/bossie/Documents/ston")
    } finally {
      sc.stop()
    }
  }
}
