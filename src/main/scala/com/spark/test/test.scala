package com.spark.test
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
object test {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main (args: Array[String] ): Unit = {
    val masterUrl = "local[2]"
    //val sparkconf = new SparkConf ().setAppName ("sparktestApp").setMaster(masterUrl)
    val spark=SparkSession.builder()
      .master("local[2]")
      .appName("UnderstandingSparkSession")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()
    //spark配置，建议保留setMaster(local)

    //调试的时候需要，在实际集群上跑的时候可在命令行自定义



    val zipsDF = spark.read.json("C:\\Users\\Administrator\\Desktop\\test.json")
    zipsDF.show()
    zipsDF.printSchema()
    zipsDF.createOrReplaceTempView("Ter_people")
    //val result=spark.sql("select * from Ter_people limit 5")
    //result.show()
    //val sc = new SparkContext (sparkconf)
    //val sqlContext = new SQLContext(sc)
    //val dfs = sqlContext.read.json("C:\\Users\\Administrator\\Desktop\\test.json")
    //dfs.show()
    /*    val sc = new SparkContext (sparkconf)
        val rdd = sc.parallelize (List (1, 2, 3, 4, 5, 6) ).map (_* 3) //将数组(1,2,3,4,5,6)分别乘3
        rdd.filter (_> 10).collect ().foreach (println) //打印大于10的数字
        println (rdd.reduce (_+ _) )//打印 和
        println ("hello world")*/

  }
}
