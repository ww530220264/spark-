//package com.ww.examples.streaming
//
//import com.ww.examples.SparkUtils
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object RawNetworkGrep {
//
//  def main(args: Array[String]): Unit = {
//
//    if (args.length < 4) {
//      System.err.println("Usage: RawNetworkGrep <batch interval> <num streams> <hostname> <port>")
//      System.exit(1)
//    }
//
//    val sparkConf = SparkUtils.getSparkConf("ww-RawNetworkGrep")
//    val ssc = new StreamingContext(sparkConf, Seconds(args(0).toInt))
//    //    ssc.sparkContext.setLogLevel("ERROR")
//
////    val rawStrems = (1 to args(1).toInt).map { _ =>
////      ssc.rawSocketStream[String](args(2), args(3).toInt, StorageLevel.MEMORY_AND_DISK_2)
////    }.toArray
////    val union = ssc.union(rawStrems)
////    union.filter(_.contains("the")).count().foreachRDD(rdd => {
////      System.err.println(s"Grep count: -------${rdd.collect().mkString}")
////    })
//
//    ssc.socketTextStream(args(2), args(3).toInt, StorageLevel.MEMORY_AND_DISK_2).filter(_.contains("the")).print()
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
