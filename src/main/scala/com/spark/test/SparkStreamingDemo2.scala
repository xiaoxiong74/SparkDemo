package com.spark.test
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingDemo2 {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf对象，并指定AppName和Master
    val conf = new SparkConf().setAppName("StreamingDemo2").setMaster("local[2]")
    // 创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(10))

    val hostname = "192.168.109.230" // 即我们的master虚拟机
    val port = 9999 // 端口号

    // 创建DStream对象
    val lines = ssc.socketTextStream(hostname, port)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    wordCounts.print()
    println(wordCounts)
    ssc.start()
    ssc.awaitTermination()

  }
}
