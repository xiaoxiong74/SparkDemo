package com.spark.test
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkConf

object SparkStreamingDemo1 {
  def main(args: Array[String]): Unit ={
    val sparkConf = new SparkConf().setAppName("HDFSWordCount").setMaster("local[2]")

    //create the streaming context
    val  ssc = new StreamingContext(sparkConf, Seconds(10))

    //process file when new file be found.
    //新文件不能复制产生，可通过notepad另存或代码创建文件
    val lines = ssc.textFileStream("C:\\Users\\Data\\testdata")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)//这里不是rdd,而是dstream
    wordCounts.print()
    println(wordCounts)
    ssc.start()
    ssc.awaitTermination()
  }
}
