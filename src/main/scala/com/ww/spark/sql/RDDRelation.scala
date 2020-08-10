package com.ww.spark.sql

import com.ww.spark.SparkUtils
import org.apache.spark.sql.SaveMode

case class Record(key: Int, value: String)

object RDDRelation {
  def main(args: Array[String]): Unit = {
    val sparkConf = SparkUtils.getSparkConf("ww-RDDRelation")
    val spark = SparkUtils.getSparkSession(sparkConf)
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val df = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    df.createOrReplaceTempView("records")
    println("Results of select *: ")
    spark.sql("select * from records").collect().foreach(println)
    println("--------------------------------")
    val count = spark.sql("select count(*) from records").collect().head.getLong(0)
    println(s"COUNT(*): $count")
    println("--------------------------------")
    val rddFromSql = spark.sql("select key,value from records where key <= 10")
    rddFromSql.write.mode(SaveMode.Overwrite).parquet("./RDDRelation/pair.parquet")

    val parquetFile = spark.read.parquet("./RDDRelation/pair.parquet")
    parquetFile.where($"key" === 1).select($"value".as("a")).collect().foreach(println)
    println("--------------------------------")
    parquetFile.createOrReplaceTempView("parquetFile")
    spark.sql("select * from parquetFile").collect().foreach(println)
    println("--------------------------------")
    spark.stop()
  }
}
