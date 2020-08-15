package com.ww.spark.streaming.kafka

import com.ww.spark.SparkUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object KafkaStreamingExampleRecoverable {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "spark")
    val consumerConfig = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "cdh1:9092,cdh2:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.GROUP_ID_CONFIG -> "KafkaStreamingExampleRecoverable_1",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )
    val checkpointDir = "./KafkaStreamingExampleRecoverable"
    val topics = Seq("user_order")
    val ssc = StreamingContext.getOrCreate(checkpointDir, () => createSSCContext(checkpointDir, consumerConfig, topics))
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.start()
    ssc.awaitTermination()
  }

  def createSSCContext(checkpointDir: String, consumerConfig: Map[String, String], topics: Seq[String]): StreamingContext = {
    val sparkConf = SparkUtils.getSparkConf("hx-KafkaStreamingExampleRecoverable")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint(checkpointDir)

    val kafkaStreams = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, consumerConfig))

    val A: mutable.HashMap[String, Array[OffsetRange]] = new mutable.HashMap()
    kafkaStreams.transform(rdd => {
      A += "rdd" -> rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    kafkaStreams.map(_.value()).foreachRDD(rdd => {
      val spark = SparkUtils.getSparkSession(rdd.sparkContext.getConf)
      import spark.implicits._
      val df = spark.read.json(spark.createDataset(rdd))
      df.select("*").show()

      if (scala.util.Random.nextInt(5) == 2) {
        1 / 0
      }

      kafkaStreams.asInstanceOf[CanCommitOffsets].commitAsync(A.get("rdd").getOrElse(new Array[OffsetRange](0)))
    })

    ssc
  }
}
