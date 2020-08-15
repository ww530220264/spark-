package com.ww.spark.streaming.kafka


import com.ww.spark.SparkUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{StringDeserializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.mutable

/**
 * json字符串类型RDD转DataFrame
 * val rows = spark.sparkContext.parallelize(Seq("{'user_id':369,'order_id':7530196,'categroy_id':54,'product_id':2281,'product_num':3,'amount':71.4,'favorites':['a','b']}"))
 * val df = spark.read.json(spark.createDataset(rows))
 */
object KafkaStreamingExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = SparkUtils.getSparkConf("ww-KafkaStreamingExample")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")

    val consumerConfig = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "cdh1:9092,cdh2:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.GROUP_ID_CONFIG -> "KafkaStreamingExample_1",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    val kafkaStream = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Seq("user_order"), consumerConfig))
    val A: mutable.HashMap[String, Array[OffsetRange]] = new mutable.HashMap()
    val transformStream = kafkaStream.transform(rdd => {
      A += "rdd" -> rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    transformStream.map(_.value()).foreachRDD(rdd => {
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val df = spark.read.json(spark.createDataset(rdd))
      df.show()
      kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(A.get("rdd").getOrElse(new Array[OffsetRange](0)))
    })
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        System.err.println("-" * 50)
        System.err.println("..........JVM退出........")
        System.err.println("-" * 50)
      }
    }))
    ssc.start()
    ssc.awaitTermination()
  }
}
