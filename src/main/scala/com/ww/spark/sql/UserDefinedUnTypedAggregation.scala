package com.ww.spark.sql

import com.ww.spark.SparkUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

object UserDefinedUnTypedAggregation {

  object MyAverage extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

    override def bufferSchema: StructType = StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)

    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0).toDouble / buffer.getLong(1)
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = SparkUtils.getSparkConf("ww-UserDefinedUnTypedAggregation")
//    sparkConf.setMaster("local[*]")
//    sparkConf.remove("spark.driver.host")
    val spark = SparkUtils.getSparkSession(sparkConf)
    spark.sparkContext.setLogLevel("ERROR")
    // name中间不要有特殊符号
    spark.udf.register("averageSalary", MyAverage)

    val empDF = spark.read.json("/ww-temp/datasets/employees.json")
    empDF.createOrReplaceTempView("employee")
    spark.sql("select averageSalary(salary) from employee").show()

    spark.stop()
  }

}
