package com.ww.spark.streaming

import com.ww.spark.SparkUtils
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object WordCountNetWorkStateful {
  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println("Usage: WordCountNetWorkStateful <hostname> <port> <wordBaseCount> <logLevelString>")
      System.exit(1)
    }

    val sparkConf = SparkUtils.getSparkConf("ww-WordCountNetworkStateful")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("./WordCountNetWorkStateful")
    // 设置日志级别
    ssc.sparkContext.setLogLevel("ERROR")
    //
    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))
    val lines = ssc.socketTextStream(args(0), args(1).toInt)
    val words = lines.flatMap(_.split("\\W+"))
    val wordDStream = words.map((_, args(2).toInt))
    // keyType,Option[valueType],State[stateType]
    val mapFun = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption().getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    var stateDstrem: DStream[(String, Int)] = null
    if (args(4).toBoolean) {
      stateDstrem = wordDStream
        .mapWithState(StateSpec.function(mapFun).initialState(initialRDD))
        .stateSnapshots()
    } else {
      // 如果不加stateSnapshots(),则流中只包含当前窗口中存在的key的数据
      stateDstrem = wordDStream
        .mapWithState(StateSpec.function(mapFun).initialState(initialRDD))
    }

    stateDstrem.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
