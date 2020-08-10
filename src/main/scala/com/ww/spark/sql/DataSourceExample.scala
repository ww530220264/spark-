package com.ww.spark.sql

import java.util.Properties

import com.ww.spark.SparkUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSourceExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = SparkUtils.getSparkConf("ww-DataSourceExample")
    sparkConf.setJars(List("C:\\workspace\\cdh\\target\\original-cdh-1.0-SNAPSHOT.jar",
      "D:\\repository\\mysql\\mysql-connector-java\\5.1.47\\mysql-connector-java-5.1.47.jar"))
    val spark = SparkUtils.getSparkSession(sparkConf)
    spark.sparkContext.setLogLevel("ERROR")

    loadFromMysql(spark)

    spark.stop()
  }

  def loadFromMysql(spark: SparkSession): Unit = {
    val mysqlDF = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.10.247:3309/xloa_employee?useUnicode=true&characterEncoding=utf8&autoReconnect=true")
      .option("dbtable", "(select cast(username as signed) as username, employee_name as employeeName from hr_employ where username > 8000) as tmp")
      .option("user", "hxtech")
      .option("password", "hxtech@2020")
      .option("partitionColumn", "username")
      .option("lowerBound", "8001")
      .option("upperBound", "8342")
      .option("numPartitions", 3)
      .load()

    println(s"总数: ${mysqlDF.count()}")

    mysqlDF.show(30)
    import spark.implicits._

    mysqlDF.select($"username", $"employeeName").write.mode(SaveMode.Overwrite)
      .format("jdbc")
      .option("createTableColumnTypes", "username INTEGER, employeeName VARCHAR(16)")
      .option("url", "jdbc:mysql://192.168.10.247:3309/xloa_employee?useUnicode=true&characterEncoding=utf8&autoReconnect=true")
      .option("dbtable", "xloa_employee.hr_employ_111111")
      .option("user", "hxtech")
      .option("password", "hxtech@2020")
      .save()
//    后面的每个条件相当于一个分区,可以作用于非数值类
//    spark.read.jdbc("url", "table", Array[String]("username <= 8100", "username > 8100 and username <= 9000"),new Properties())
  }
}
