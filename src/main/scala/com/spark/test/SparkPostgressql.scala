package com.spark.test
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Properties

object SparkPostgressql {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("FromPostgreSql")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.sql.shuffle.partitions","12")
      .setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    //sqlContext.setConf("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
    val table = "colortype";
    //增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
    val prop = new Properties();
    prop.put("user","postgres");
    prop.put("password","postgres");
    prop.put("driver","org.postgresql.Driver")
   // val query = "SELECT * FROM groups"
    val url = "jdbc:postgresql://192.168.105.236:5432/cqzatest"
    val jdbcDF = sqlContext.read.jdbc(url, table, prop).select("*")
    jdbcDF.show()
/*    val jdbcDF = sqlContext.read.format("jdbc").options(Map(
      "url" -> url,
      //"driver" -> "org.postgresql.Driver",
      "dbtable" -> "events")).load()*/
/*   val users = sqlContext.load("jdbc", Map(
      "url" -> url,
      "driver" -> "org.postgresql.Driver",
      "dbtable" -> query
    ))*/

    //jdbcDF.show()
    //println(users)
    //jdbcDF.foreach(row=>print(row))
  }

}
