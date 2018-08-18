package com.spark.test
import kafka.api.OffsetRequest
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
object SparkStreamingKafkaDemo1 {
  Logger.getRootLogger.setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {

    // 创建SparkConf对象，并指定AppName和Master
    val conf = new SparkConf()
      .setAppName("StreamingReadData")
      .setMaster("local[3]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // 创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(2))

    //    val zkServers = "master:2181,slave1:2181,slave2:2181"
    // 注意：需要在本机的hosts文件中添加 master/slave1/slave2对应的ip
    val brokers = "node1.hde.h3c.com:6667,node2.hde.h3c.com:6667,node3.hde.h3c.com:6667"

    val topics = "hszalog"
    val groupId = "consumer_001"

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      "bootstrap.servers"-> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)// 说明每次程序启动，从kafka中最开始的第一条消息开始读取
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    val lines=stream.map(record => (record.key, record.value))
    lines.print()

    ssc.start()
    ssc.awaitTermination()
  }
}