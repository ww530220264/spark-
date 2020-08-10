package com.ww.spark.sql

import com.ww.spark.SparkUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SQLExample {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val sparkConf = SparkUtils.getSparkConf("ww-RDDExample")
    val spark = SparkUtils.getSparkSession(sparkConf)
    spark.sparkContext.setLogLevel("ERROR")

    dataFrameExample(spark)
    dataSetExample(spark)
    inferSchemaExample(spark)
    programmaticSchemaExample(spark)
    spark.stop()
  }

  def dataFrameExample(spark: SparkSession): Unit = {
    val path = "/hx-tmp/spark-examples-data/people.json"
    val df = spark.read.json(path)
    df.show()
    println("-" * 50)
    import spark.implicits._
    println("-" * 50)
    df.printSchema()
    println("-" * 50)
    df.select($"name").show()
    println("-" * 50)
    df.select($"name", $"age" + 1).show()
    println("-" * 50)
    df.filter($"age" > 21).show()
    println("-" * 50)
    df.groupBy($"age").count().show()
    println("-" * 50)
    df.createOrReplaceTempView("people")
    spark.sql("select * from people").show()
    println("-" * 50)
    df.createGlobalTempView("people")
    spark.sql("select * from global_temp.people").show()
    println("-" * 50)
    spark.newSession().sql("select * from global_temp.people").show()
  }

  def dataSetExample(spark: SparkSession): Unit = {
    import spark.implicits._
    val ds_1 = Seq(Person("Andy", 32)).toDS()
    ds_1.show()
    println("-" * 50)
    val ds_2 = Seq(1, 2, 3).toDS()
    ds_2.map(_ + 1).collect().foreach(println)
    println("-" * 50)
    val path = "/hx-tmp/spark-examples-data/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()
    println("-" * 50)
  }

  def inferSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._
    val path = "/hx-tmp/spark-examples-data/people.txt"
    val peopleDF = spark.sparkContext.textFile(path)
      .map(_.split(","))
      .map(attrs => Person(attrs(0), attrs(1).trim.toInt))
      .toDF()
    peopleDF.createOrReplaceTempView("people")
    val teenagerDF = spark.sql("select * from people where age between 13 and 19")
    teenagerDF.map(t => "Name: " + t(0)).show()
    println("-" * 50)
    teenagerDF.map(t => "Name: " + t.getAs[String]("name")).show()
    println("-" * 50)
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    teenagerDF.map(t => t.getValuesMap(List("name", "age"))).collect()
    println("-" * 50)
  }

  def programmaticSchemaExample(spark: SparkSession) = {
    import spark.implicits._
    val schemaString = "name,age"
    val structFields = schemaString.split(",").map(x => StructField(x, StringType, true))
    val schema = StructType(structFields)
    val path = "/hx-tmp/spark-examples-data/people.txt"
    val personRDD = spark.sparkContext.textFile(path)
    val rowRDD = personRDD.map(_.split(",")).map(x=>Row(x(0).trim,x(1).trim))
    val peopleDF = spark.createDataFrame(rowRDD, schema)
    peopleDF.show()
    peopleDF.printSchema()
    peopleDF.createOrReplaceTempView("people")
    val result = spark.sql("select name from people")
    result.map(attrs=>"Name: " + attrs(0)).show()
      println("-" * 50)
  }
}
