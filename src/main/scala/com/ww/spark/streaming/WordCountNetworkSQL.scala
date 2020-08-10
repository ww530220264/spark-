package com.ww.spark.streaming

import com.ww.spark.{SparkSessionSingleton, SparkUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext, Time}

object WordCountNetworkSQL {
  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("Usage: <host> <port> <batch interval>")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt
    val duration = args(2).toInt

    val sparkConf = SparkUtils.getSparkConf("ww-WordcountNetworkSQL")
    val ssc = new StreamingContext(sparkConf, Seconds(duration))
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint("./WordCountNetworkSQL")
    val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_2)
    val words = lines.flatMap(_.split("\\W+")).map((_, 1))
    val mapFun = (key: String, one: Option[Int], state: State[Int]) => {
      val total = one.getOrElse(0) + state.getOption().getOrElse(0)
      state.update(total)
      (key, total)
    }
    val statefulStream = words.mapWithState(StateSpec.function(mapFun)).stateSnapshots()
    statefulStream.foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
      val spark = SparkSessionSingleton.getInstannce(sparkConf)
      import spark.implicits._

      val df = rdd.map(x => Record(x._1, x._2)).toDF()
      df.createOrReplaceTempView("words")
      val dataFrame = spark.sql("select word,cnt from words")
      System.err.println(s"========$time=============")
      dataFrame.show()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

case class Record(word: String, cnt: Long)