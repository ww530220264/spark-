package com.ww.spark.sql.streaming

import java.sql.Timestamp

import com.ww.spark.SparkUtils
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object NetworkWordCountStructedStreaming {
  def main(args: Array[String]): Unit = {
    //    val sparkConf = SparkUtils.getSparkConf("ww-NetworkWordCountStructedStreaming")
    //    sparkConf.set("spark.sql.streaming.checkpointLocation","./NetworkWordCountStructedStreaming")
    //    val spark = SparkSession.builder()
    //      .master("local[*]")
    //      .appName("ww-NetworkWordCountStructedStreaming")
    //      .config("spark.sql.streaming.checkpointLocation", "file:///C:\\workspace\\cdh\\checkpoint\\NetworkWordCountStructedStreaming")
    //      .getOrCreate()
    val sparkConf = SparkUtils.getSparkConf("")
      .set("spark.sql.streaming.checkpointLocation", "./NetworkWordCountStructedStreaming")
    val spark = SparkUtils.getSparkSession(sparkConf)
    spark.sparkContext.setLogLevel("ERROR")

    //    wordcountWindow(spark)
//    wordcountWindowWithWatermark(spark)
    joinStringWindowWithWatermark(spark)

  }

  def joinStringWindowWithWatermark(spark: SparkSession): Unit = {
    import spark.implicits._
    var df1 = spark.readStream.format("socket").option("host", "192.168.10.151").option("port", 9999).load()
    df1 = df1.as[String].map(_.split("\\W+")).filter(_.length >= 3).map(x => (new Timestamp(x(0).toLong), x(1), x(2))).toDF("timestamp1", "key1", "value1")
    var df2 = spark.readStream.format("socket").option("host", "192.168.10.151").option("port", 9998).load()
    df2 = df2.as[String].map(_.split("\\W+")).filter(_.length >= 3).map(x => (new Timestamp(x(0).toLong), x(1), x(2))).toDF("timestamp2", "key2", "value2")
    df1 = df1.withWatermark("timestamp1", "5 seconds")
    df2 = df2.withWatermark("timestamp2", "8 seconds")
    import org.apache.spark.sql.functions.expr
    // spark.sql.streaming.multipleWatermarkPolicy=max/min
    // 默认为min,,两个流中较小的watermark作为全局watermark,防止当一个流比另一个流慢的时候,慢的流中的元素被drop
    val resultDF = df1.join(df2, expr(
      """
        key1 = key2 AND
        timestamp1 >= timestamp2 AND
        timestamp1 <= timestamp2 + interval 3 second
        """))
    import scala.concurrent.duration._
    val query = resultDF
      .writeStream
      .option("truncate", false)
      .format("console")
      .trigger(Trigger.ProcessingTime(3.seconds))
      .outputMode(OutputMode.Append()) // 两个流Join只支持Append模式
      .start()

    query.awaitTermination()

  }

  def wordcountWindowWithWatermark(spark: SparkSession): Unit = {
    val linesDF = spark
      .readStream
      .format("socket")
      .option("host", "192.168.10.151")
      .option("port", 9999)
      .load()
    import spark.implicits._
    import scala.concurrent.duration._
    val wordDS = linesDF.as[String].map(_.split("\\W+")).map(x => (new Timestamp(x(0).toLong), x(1)))
    val wordDF = wordDS.toDF("timestamp", "word")
    val query = wordDF
      .withWatermark("timestamp", "10 seconds")
      .groupBy(window($"timestamp", "10 seconds", "5 seconds"), $"word")
      .count()
      .writeStream
      .option("truncate", false)
      .format("console")
      //      .outputMode("complete")
      // 当有窗口被触发时,才会输出触发的窗口聚合结果,会考虑延迟到达的数据,超过延迟时间之后到达的数据,对应的窗口已经被触发,不会再处理这个数据
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(3.seconds))
      // 输出新增或更新的窗口的聚合数据,会考虑延迟到达的数据,超过延迟时间之后到达的数据,对应的窗口已经被触发,不会再处理这个数据
      //      .outputMode("update")
      .start()

    query.awaitTermination()
  }

  def wordcountWindow(spark: SparkSession): Unit = {
    //    case class MyEvent(timestamp: Long, word: String)
    val linesDF = spark
      .readStream
      .format("socket")
      .option("host", "192.168.10.151")
      .option("port", 9999)
      .load()
    import spark.implicits._
    //    implicit val myEndoder = Encoders.kryo(classOf[MyEvent])
    val eventDS = linesDF.as[String].map(_.split("\\W+")).map(x => (new Timestamp(x(0).toLong), x(1)))
    val windowDF = eventDS.toDF("timestamp", "word")
      .groupBy(window($"timestamp", "10 seconds", "5 seconds"), $"word")
      .count()
    val query = windowDF
      .writeStream
      .option("truncate", false)

      /**
       * +------------------------------------------+----+-----+
       * |window                                    |word|count|
       * +------------------------------------------+----+-----+
       * |[1970-01-01 08:00:00, 1970-01-01 08:00:10]|b   |2    |
       * |[1970-01-01 08:00:05, 1970-01-01 08:00:15]|b   |2    |
       * |[1970-01-01 08:00:10, 1970-01-01 08:00:20]|c   |1    |
       * |[1970-01-01 08:00:05, 1970-01-01 08:00:15]|a   |1    |
       * |[1970-01-01 08:00:05, 1970-01-01 08:00:15]|c   |1    |
       * |[1970-01-01 08:00:00, 1970-01-01 08:00:10]|a   |2    |
       * |[1970-01-01 08:00:05, 1970-01-01 08:00:15]|d   |1    |
       * |[1970-01-01 08:00:00, 1970-01-01 08:00:10]|d   |1    |
       * |[1970-01-01 07:59:55, 1970-01-01 08:00:05]|a   |1    |
       * +------------------------------------------+----+-----+
       */
      //      .outputMode("complete") // 输出所有窗口的聚合结果
      //      .outputMode("append") // 没有watermark但存在aggregation时[不支持追加模式]
      /**
       * +------------------------------------------+----+-----+
       * |window                                    |word|count|
       * +------------------------------------------+----+-----+
       * |[1970-01-01 08:00:00, 1970-01-01 08:00:10]|b   |2    |
       * |[1970-01-01 08:00:05, 1970-01-01 08:00:15]|b   |1    |
       * +------------------------------------------+----+-----+
       * +------------------------------------------+----+-----+
       * |window                                    |word|count|
       * +------------------------------------------+----+-----+
       * |[1970-01-01 08:00:00, 1970-01-01 08:00:10]|b   |3    |
       * |[1970-01-01 07:59:55, 1970-01-01 08:00:05]|b   |2    |
       * +------------------------------------------+----+-----+
       */
      .outputMode("update") // 输出新增的或更新的窗口的聚合结果
      .format("console")
      .start()

    query.awaitTermination()
  }

  def wordcount(spark: SparkSession): Unit = {
    val linesDF = spark
      .readStream
      .format("socket")
      .option("host", "192.168.10.151")
      .option("port", 9999)
      .load()
    import spark.implicits._
    val wordsDS = linesDF.as[String].flatMap(_.split("\\W+"))
    val wordCountDF = wordsDS.groupBy($"value").count()
    val query = wordCountDF
      .writeStream
      //      .outputMode("complete") // 支持 输出所有的聚合结果
      .outputMode("update") // 支持,只输出有新增或更新的聚合结果
      //      .outputMode("append") // 没有watermark但存在aggregation时[不支持追加模式]
      .format("console")
      .start()
    query.awaitTermination()
  }
}
