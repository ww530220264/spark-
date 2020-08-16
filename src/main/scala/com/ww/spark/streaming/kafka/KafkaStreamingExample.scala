package com.ww.spark.streaming.kafka


import java.util

import com.ww.hbase.HbaseUtils
import com.ww.spark.SparkUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.mutable
import scala.util.parsing.json.JSON

/**
 * json字符串类型RDD转DataFrame
 * val rows = spark.sparkContext.parallelize(Seq("{'user_id':369,'order_id':7530196,'categroy_id':54,'product_id':2281,'product_num':3,'amount':71.4,'favorites':['a','b']}"))
 * val df = spark.read.json(spark.createDataset(rows))
 */
object KafkaStreamingExample {

  case class User_Record(user_id: String, order_id: String, categroy_id: Int, product_id: String, product_num: Int, amount: Double)

  def main(args: Array[String]): Unit = {
    val sparkConf = SparkUtils.getSparkConf("ww-KafkaStreamingExample")
//      .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")

    ssc.checkpoint("./KafkaStreamingExample_1")

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
    transformStream.map(r => (r.value(), r.timestamp())).foreachRDD(rdd => {
      rdd.foreachPartition(items => {
        if (items.nonEmpty) {
          val threadConnection = HbaseUtils.getThreadConnection()
          var table: Table = null
          try {
            table = threadConnection.getTable(TableName.valueOf("user_order"))
            items.foreach(item => {
              val recordMap = JSON.parseFull(item._1).get.asInstanceOf[Map[String, String]]
              val record: User_Record = User_Record(
                recordMap.get("user_id").get,
                recordMap.get("order_id").get,
                recordMap.get("categroy_id").get.toInt,
                recordMap.get("product_id").get,
                recordMap.get("product_num").get.toInt,
                recordMap.get("amount").get.toDouble
              )
              val timestamp = item._2
              val put = new Put(Bytes.toBytes(record.user_id + (Long.MaxValue - timestamp)))
              put.addColumn(Bytes.toBytes("order"), Bytes.toBytes("user_id"), Bytes.toBytes(record.user_id))
              put.addColumn(Bytes.toBytes("order"), Bytes.toBytes("order_id"), Bytes.toBytes(record.order_id))
              put.addColumn(Bytes.toBytes("order"), Bytes.toBytes("categroy_id"), Bytes.toBytes(record.categroy_id))
              put.addColumn(Bytes.toBytes("order"), Bytes.toBytes("amount"), Bytes.toBytes(record.amount))
              put.addColumn(Bytes.toBytes("product"), Bytes.toBytes("product_id"), Bytes.toBytes(record.product_id))
              put.addColumn(Bytes.toBytes("product"), Bytes.toBytes("product_num"), Bytes.toBytes(record.product_num))
              try {
                table.put(put)
              } catch {
                case ex: Throwable => {
                  ex.printStackTrace()
                  table.put(put)
                }
              }
            })
          } finally {
            if (null != table) {
              table.close()
            }
          }
        }
      })
      kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(A.get("rdd").getOrElse(new Array[OffsetRange](0)))
    }

    )
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
