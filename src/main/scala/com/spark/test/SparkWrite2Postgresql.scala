package com.spark.test
import java.util.Properties
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
/**
  * todo:sparksql写入数据到postgresql中
  */

case class Student(id:Int,name:String,sex:String,age:Int,course:String)
object SparkWrite2Postgresql {
  def main(args: Array[String]): Unit = {
    //todo:1、创建sparkSession对象
    val spark: SparkSession=SparkSession.builder()
      .master("local[2]")
      .appName("SparkWrite2Postgresql")
      .getOrCreate()
    //todo:2、读取数据
    val data: RDD[String]=spark.sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\hive_test\\students.txt")
    //todo:分割数据
    val student=data.map(_.split(",")).map(x=>Student(x(0).toInt,x(1),x(2),x(3).toInt,x(4)))
    //todo:导入隐式转换
    import spark.implicits._
    //todo:rdd转为dataframe
    val studentDF=student.toDF()
    //todo:注册成表
    studentDF.createOrReplaceTempView("student1")
    //todo:查询
    val resultDF=spark.sql("select * from student1 order by age desc")
    resultDF.show()

    //todo:将结果保存到postgresql
    //todo:创建Properties对象，配置连接postgresql的用户名和密码
    val prop=new Properties()
    prop.setProperty("user","postgres")
    prop.setProperty("password","postgres")

    //todo:写入数据库（表不用在数据库建立，自动创建，若数据库中已建好表则会报表存在的错误）

    resultDF.write.jdbc("jdbc:postgresql://192.168.105.236:5432/cqzatest","student",prop)
    //todo:写入mysql时，可以配置插入mode，overwrite覆盖，append追加，ignore忽略，error默认表存在报错
    //resultDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.200.150:3306/spark","student",prop)
    spark.stop()
  }
}
