package com.spark.test
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}


object SparkSessionDemo1 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main (args: Array[String] ): Unit = {
    val masterUrl = "local[2]"
    //val sparkconf = new SparkConf ().setAppName ("sparktestApp").setMaster(masterUrl)
    val spark=SparkSession.builder()
      .master("local[2]")
      .appName("UnderstandingSparkSession")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    //spark配置，建议保留setMaster(local)
    //调试的时候需要，在实际集群上跑的时候可在命令行自定义


    val customSchema = StructType(Array(
      StructField("TimeStamp", StringType, nullable = true),
      StructField("Temperature", DoubleType, nullable = true)
    )
    )
    val df = spark.read.format("com.databricks.spark.csv")
      .schema(customSchema)
      .load("C:\\Users\\Administrator\\Desktop\\WORK\\spark_test\\data.csv")

    //时间格式：2010-02-25T03:33:21.000Z	79.77
    //截取时间
    def getTime(timeStamp: String) = {
      val newtime=timeStamp.slice(11,16)
      newtime
    }
    val parseTime = udf(getTime _, StringType)

    //分区 按日期与分钟分区
    val partition: Unit=df.withColumn("time",parseTime(df.col("Timestamp"))).withColumn("date", to_date(df.col("TimeStamp")))
      .write
      .partitionBy("date")
      .partitionBy("time")
      .format("parquet")
      .mode("Ignore")
      .save("hdfs://node1.hde.h3c.com:8020/user/kf7899/MachineResultInCommon.parquet")

    val parquetFileByDateDF: Unit = spark.read.parquet("hdfs://node1.hde.h3c.com:8020/user/kf7899/MachineResultInCommon.parquet")
      .createOrReplaceTempView("pfile")

    val timeTemperaturequery: Unit =spark.sql("select * from pfile WHERE time BETWEEN '03:30' and '08:30' and date='2010-02-25'")
      .show()
//    val zipsDF = spark.read.json("C:\\Users\\Administrator\\Desktop\\test.json")
//    zipsDF.show()
//    zipsDF.printSchema()
//    zipsDF.createOrReplaceTempView("Ter_people")
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
