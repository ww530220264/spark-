package com.ww.spark.sql

import com.ww.spark.SparkUtils
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator


object UserDefinedTypedAggregation {

  case class Employee(name: String, salary: Long)

  case class Average(var sum: Long, var count: Long)

  object MyAverage extends Aggregator[Employee, Average, Double] {

    override def zero: Average = Average(0L, 0L)

    override def reduce(b: Average, a: Employee): Average = {
      b.sum += a.salary
      b.count += 1
      b
    }

    override def merge(b1: Average, b2: Average): Average = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    override def finish(reduction: Average): Double = {
      reduction.sum.toDouble / reduction.count
    }

    override def bufferEncoder: Encoder[Average] = Encoders.product[Average]

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = SparkUtils.getSparkConf("ww-UserDefinedTypedAggregation")
    sparkConf.setMaster("local[*]")
    sparkConf.remove("spark.driver.host")
    val spark = SparkUtils.getSparkSession(sparkConf)
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    val empDF = spark.read.json("file:///C:\\ww\\cdh\\src\\main\\resources\\basic-datasource\\employees.json")
    empDF.show()
    val empDS = empDF.as[Employee]
    val typedColumn = MyAverage.toColumn.name("average-salary")
    val result = empDS.select(typedColumn)
    result.show()

    spark.stop()
  }

}
