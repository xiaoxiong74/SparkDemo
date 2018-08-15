package com.spark.test
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
object test1 {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val logFile = "hdfs://node1.hde.h3c.com:8020/user/data/test.txt"
    //val logFile = "C:\\Users\\Data\\test.txt" // Should be some file on your system
    val spark = SparkSession.builder.master("local[1]").appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
