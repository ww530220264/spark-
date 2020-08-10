package com.ww.kafka

import java.util.Properties

import scala.util.Random
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

object MyProducer {

  @volatile private var flag: Boolean = true

  val config = new Properties()
  config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cdh2:9092")
  config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  config.put(ProducerConfig.ACKS_CONFIG, "all")
  //  config.put(ProducerConfig.RETRIES_CONFIG, "3")
  config.put(ProducerConfig.RETRIES_CONFIG, Int.MaxValue.toString)

  val producer: KafkaProducer[String, String] = new KafkaProducer(config)

  def main(args: Array[String]): Unit = {
    send_async()
  }

  def send_async(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        try {
          var i = 1000000
          while (i > 0) {
            val user_id = Random.nextInt(20000) + 1
            val order_id = Random.nextInt(10000000)
            val categroy_id = Random.nextInt(100) + 1
            val product_id = Random.nextInt(10000) + 1
            val product_num = Random.nextInt(10) + 1
            val amount = Random.nextInt(5000) / 10.0
            val value = s"{'user_id':${user_id},'order_id':${order_id},'categroy_id':${categroy_id},'product_id':${product_id},'product_num':${product_num},'amount':${amount},}"
            producer.send(new ProducerRecord[String, String]("user_order", value), new ProducerCallback())
            i -= 1
            Thread.sleep(Random.nextInt(1000))
          }
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
    }).start()
    System.err.println("请输入...")
    val a = System.in.read()
    if (a == 49) {
      flag = false
      System.err.println("------------")
      Thread.sleep(5000)
    }
    producer.close()
  }

  class ProducerCallback() extends Callback {
    override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
      println(Thread.currentThread() + "----")
      if (e != null) {
        e.printStackTrace()
        System.err.println("发送失败....")
      } else {
        System.err.println("发送成功....")
      }
    }
  }

}
