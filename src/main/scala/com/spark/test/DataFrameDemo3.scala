package com.spark.test
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row

//对于非结构化的数据
/*people.txt
1,Ganymede,32
2, Lilei, 19
3, Lily, 25
4, Hanmeimei, 25
5, Lucy, 37
6, wcc, 4*/

object DataFrameDemo3 {
  case class People(id: Int,name:String,age:Int)  //方法二用到
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //初始化
    val conf = new SparkConf().setAppName("DataFrameTest3").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val people = sc.textFile("hdfs://node1.hde.h3c.com:8020/user/data/people1.txt")

//方法一  通过字段反射来映射注册临时表
/*

    val peopleRowRDD = people.map { x =>
      x.split(",")
    }.map { data => {
      val id = data(0).trim().toInt
      val name = data(1).trim()
      val age = data(2).trim().toInt
      Row(id, name, age)
    }
    }
    val structType = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)))
    val df=sqlContext.createDataFrame(peopleRowRDD,structType)
    df.registerTempTable("people")
    //df.createTempView("people")
    df.show()

*/
   //方法二 通过case class反射来映射注册临时表
    val perpleRDD=people.map{x=>
  x.split(",")}.map{data=>
  {
    People(data(0).trim().toInt,data(1).trim(),data(2).trim().toInt)
  }
}
    //这里需要隐式转换
    import sqlContext.implicits._  //导入隐式的转化函数
    val df = perpleRDD.toDF()
    df.registerTempTable("people")
    //df.createTempView("people")
    df.show()

  }
}
