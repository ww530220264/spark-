package com.ww.spark

import scala.math.random

object Pi {

  def main(args: Array[String]): Unit = {

    val spark = SparkUtils.getSparkSession(SparkUtils.getSparkConf("ww-SparkPi"))
    //
    val total = 1000000
    val slices = 3
    // 落入圆中的次数
    val count = spark.sparkContext.parallelize(0 to total, slices).map { i =>
      // 0.0 < random < 1
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y <= 1) 1 else 0
    }.reduce(_ + _)
    // 正方形面积为4
    // Pi * R^2 / 4 = count / total
    val Pi = 4.0 * count / total
    println("Pi == " + Pi)
    spark.stop()
  }
}
