package com.spark.test
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

//通过注册表，操作sql的方式来操作数据


object DataFrameDemo2 {
  def main(args: Array[String]): Unit = {
    //控制日志输出级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    //初始化
    val conf=new SparkConf().setAppName("DataFrameTest2").setMaster("local[1]")
    val sc=new SparkContext(conf)
    val sqlContext= new SQLContext(sc)
    val df=sqlContext.read.json("hdfs://node1.hde.h3c.com:8020/user/data/people1.json")

    df.registerTempTable("people")
    //df.createTempView("people")
    df.show()
    df.printSchema()

    //查看某个字段
    sqlContext.sql("select name from people").show()

    //查看多个字段
    sqlContext.sql("select name,age+1 from people").show()

    //过滤某个字段
    sqlContext.sql("select name,age from people where age>=25").show()

    //cout group 某个字段的值
    sqlContext.sql("select age,count(*) from people group by age").show()

    //通过下标获取数据
    sqlContext.sql("select id,name,age from people").foreach{ x=>
      {
        println(x.get(0)+","+x.get(1)+","+x.get(2))
      }
    }

    //foreachPartition 处理个字段返回值，生产中常用的方式
    sqlContext.sql("select id,name,age from people").foreachPartition{ iterator=>
      iterator.foreach{x=> {
        println("id:"+x.getAs("id")+","+x.getAs("name")+","+x.getAs("age"))
      }
      }
    }
  }

}
