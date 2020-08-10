package com.ww.examples.streaming

import com.ww.spark.SparkUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


object QueueStream {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: QueueStream <batch interval> <oneAtTime>")
      System.exit(1)
    }

    val sparkConf = SparkUtils.getSparkConf("ww-QueueStream")
    val ssc = new StreamingContext(sparkConf, Seconds(args(0).toInt))
    ssc.sparkContext.setLogLevel("ERROR")

    val rddQueue = new mutable.Queue[RDD[Int]]()

    val inputStream = ssc.queueStream(rddQueue, args(1).toBoolean)
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    reducedStream.print()

    ssc.start()

    for (i <- 1 to 30) {
      rddQueue.synchronized {
        rddQueue += ssc.sparkContext.makeRDD(1 to 10, 10)
      }
    }

    ssc.awaitTermination()

  }
}