package com.spark.test
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

//通过DataFrame的API来操作数据

//ctrl+shift+/添加注释，则ctrl+shift+/取消注释

object DataFrameDemo1 {
  def main(args:Array[String]): Unit ={/*  people1.json
{"id":1, "name":"Ganymede", "age":32}
{"id":2, "name":"Lilei", "age":19}
{"id":3, "name":"Lily", "age":25}
{"id":4, "name":"Hanmeimei", "age":25}
{"id":5, "name":"Lucy", "age":37}
{"id":6, "name":"Tom", "age":27}*/
    //控制日志显示级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    //初始化
    val conf=new SparkConf().setAppName("DataFrameTest1").setMaster("local[1]")
    val sc=new SparkContext(conf)
    val sqlContext= new SQLContext(sc)
    val df=sqlContext.read.json("hdfs://node1.hde.h3c.com:8020/user/kf7899/people.json")
    
    //查看df中的数据
    df.show()
    //查看Schema
    df.printSchema()
    //查看某个字段
    df.select("name").show()
    //查看多个字段，plus为加上某值
    df.select(df.col("name"),df.col("age").plus(1)).show()
    //过滤某个值
    df.filter(df.col("age").gt(25)) .show()
    //count group某个字段
    df.groupBy(df.col("age")).count().show()

    println("foreach处理个字段返回值 ")
    //foreach处理个字段返回值
    df.select(df.col("id"),df.col("name"),df.col("age")).foreach{x=>
      {
        //通过下标获取数据
        println("col1:"+x.get(0)+",col2:"+x.get(1)+",clo3:"+x.get(2))
      }
    }

    println("foreachPartition 处理个字段返回值，生产中常用的方式")
    //foreachPartition 处理个字段返回值，生产中常用的方式
    df.select(df.col("id"),df.col("name"),df.col("age")).foreachPartition{iterator=>
      iterator.foreach(x=>{
        println("id:"+x.getAs("id")+",name:"+x.getAs("name")+",age:"+x.getAs("age"))
      })
    }
  }
}
