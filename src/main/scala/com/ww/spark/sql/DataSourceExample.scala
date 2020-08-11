package com.ww.spark.sql

import com.ww.spark.SparkUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSourceExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = SparkUtils.getSparkConf("ww-DataSourceExample")
      .setMaster("local[*]")
    sparkConf.setJars(List("C:\\workspace\\cdh\\target\\original-cdh-1.0-SNAPSHOT.jar"))
    val spark = SparkUtils.getSparkSession(sparkConf)
    spark.sparkContext.setLogLevel("ERROR")

    //    loadFromMysql(spark)
    loadBasicDataSource(spark)
    spark.stop()
  }

  def loadBasicDataSource(spark: SparkSession): Unit = {
    val basePath = "file:///C:\\workspace\\cdh\\src\\main\\resources\\basic-datasource\\"
    val usersDF = spark.read.parquet(basePath + "users.parquet")
    usersDF.printSchema()
    usersDF.show()
    import spark.implicits._
    usersDF
      .select($"name", $"favorite_color")
      .write
      .mode(SaveMode.Append)
      .parquet(basePath + "users_1.parquet")
    val subUsers = spark.read.parquet(basePath + "users_1.parquet")
    subUsers.printSchema()
    subUsers.show()

    val peopleDF = spark.read.format("json").load(basePath + "people.json")
    peopleDF.select($"name", $"age")
      .write
      .mode(SaveMode.Append)
      .format("json")
      .save(basePath + "people_1.json")

    val peopleCSVDF = spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(basePath + "people.csv")
    peopleCSVDF.printSchema()
    peopleCSVDF.show()

    peopleCSVDF.select($"name", $"age")
      .write
      .format("orc")
      .mode(SaveMode.Append)
      .option("orc.bloom.filter.columns", "age")
      .option("orc.dictionary.key.threshold", "1.0")
      .save(basePath + "users_with_options.orc")
    System.err.println("-" * 30)
    // 使用HIVE读取会异常,但是sparkSQL读取正常
    spark.sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "true")
    usersDF
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("favorite_color")
      .bucketBy(42, "name")
      .saveAsTable("users_partitioned_bucketed")
    // 正常读取
    spark.sql("select * from users_partitioned_bucketed").show()
  }

  // jdbc参数参考org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
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
