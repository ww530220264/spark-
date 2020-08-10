package com.ww.spark.streaming

import com.ww.spark.SparkUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountHdfs {
  def main(args: Array[String]): Unit = {

    if (args.length < 2){
      System.err.println("Usage: WordCountHdfs <batch interval> <input path>")
      System.exit(1)
    }

    val sparkConf = SparkUtils.getSparkConf("ww-WordCountHdfs")
    val ssc = new StreamingContext(sparkConf, Seconds(args(0).toInt))

    ssc.sparkContext.setLogLevel("ERROR")
    val lines = ssc.textFileStream(args(1))
    val words = lines.flatMap(_.split("\\W+"))
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
