package com.spark.test

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by jy02268879 on 2018/7/17.
  *
  * 使用Spark Streaming完成词频统计
  * 使用foreachRDD将结果写入MySQL
  *
  */
object SparkStreamingTomysql {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("UpdateStateByKey")setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //nc -lk 9999
    val lines = ssc.socketTextStream("node1.hde.h3c.com",9999)

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)


    //TODO... 将结果写入MySQL 每个partition创建连接，使用数据库连接池
    result.foreachRDD ( rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val connection = createConnection()
        partitionOfRecords.foreach(record => {
          val querySql = "SELECT t.word_count FROM wordcount t WHERE t.word = '"+record._1+"'"
          val queryResultSet = connection.createStatement().executeQuery(querySql)
          val hasNext = queryResultSet.next()
          print("MySQL had word:"+record._1+ " already  :  "+hasNext)
          if(!hasNext){
            val insertSql = "insert into wordcount(word,word_count) values('" + record._1 + "'," + record._2 + ")"
            connection.createStatement().execute(insertSql)

          }else{
            val newWordCount = queryResultSet.getInt("word_count") + record._2
            val updateSql = "UPDATE wordcount SET word_count = "+newWordCount+" where word = '"+record._1+"'"
            connection.createStatement().execute(updateSql)
          }
        })
        connection.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 获取MySQL的连接
    * */
  def createConnection()={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://192.168.108.140:3306/xiongyongfu","root","passwd")
  }

}
