package com.spark.test
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
object SparkPostgressqlDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JdbcRDDDemo1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val connection = () => {
      Class.forName("org.postgresql.Driver").newInstance()
      DriverManager.getConnection("jdbc:postgresql://192.168.105.236:5432/sonar","postgres","postgres")
    }
    //这个地方没有读取数据(数据库表也用的是person)
    val jdbcRDD = new JdbcRDD(
      sc,
      connection,
      "SELECT * FROM projects where id= ? AND id<= ?",
      //这里表示从取数据库中的第1、2、3、4条数据，然后分两个区
       1, 4, 1,
       r => {
        val id  = r.getInt(1)
        val name= r.getString(2)
        (id , name)
      }
    )
    //这里相当于是action获取到数据
    val jrdd = jdbcRDD.collect()
    jrdd.foreach(row=>print(row))
   // println(jrdd.toBuffer)
    sc.stop()
  }
}
