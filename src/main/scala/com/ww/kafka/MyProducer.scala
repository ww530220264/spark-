package com.ww.kafka

import java.util.Properties

import scala.util.Random
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

object MyProducer {

  @volatile private var flag: Boolean = true

  val config = new Properties()
  config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cdh2:9092,cdh1:9092")
  config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  config.put(ProducerConfig.ACKS_CONFIG, "all")
  //  config.put(ProducerConfig.RETRIES_CONFIG, "3")
  config.put(ProducerConfig.RETRIES_CONFIG, Int.MaxValue.toString)

  def main(args: Array[String]): Unit = {
//    val user_id = Random.nextInt(20000) + 1
//    val order_id = Random.nextInt(10000000)
//    val categroy_id = Random.nextInt(100) + 1
//    val product_id = Random.nextInt(10000) + 1
//    val product_num = Random.nextInt(10) + 1
//    val amount = Random.nextInt(5000) / 10.0
//    val value = s"{'user_id':${user_id},'order_id':${order_id},'categroy_id':${categroy_id},'product_id':${product_id},'product_num':${product_num},'amount':${amount}}"
//    println(value)
        send_async()
  }

  def send_async(): Unit = {
    System.err.println(111)
    new Thread(new Runnable {
      override def run(): Unit = {
        runProducer()
      }
    }).start()
    new Thread(new Runnable {
      override def run(): Unit = {
        runProducer()
      }
    }).start()
    new Thread(new Runnable {
      override def run(): Unit = {
        runProducer()
      }
    }).start()
    new Thread(new Runnable {
      override def run(): Unit = {
        runProducer()
      }
    }).start()

    val a = System.in.read()
    if (a == 49) {
      flag = false
      System.err.println("准备关闭------------")
    }
  }

  def runProducer(): Unit = {
    var producer: KafkaProducer[String, String] = null
    try {
      producer = new KafkaProducer(config)
      while (flag) {
        val user_id = Random.nextInt(20000) + 1
        val order_id = Random.nextInt(10000000)
        val categroy_id = Random.nextInt(100) + 1
        val product_id = Random.nextInt(10000) + 1
        val product_num = Random.nextInt(10) + 1
        val amount = Random.nextInt(5000) / 10.0
        val value = s"{'user_id':${user_id},'order_id':${order_id},'categroy_id':${categroy_id},'product_id':${product_id},'product_num':${product_num},'amount':${amount}}"
        producer.send(new ProducerRecord[String, String]("user_order", value), new ProducerCallback())
        System.err.println(value)
        Thread.sleep(Random.nextInt(5))
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    } finally {
      producer.close()
      System.err.println("producer关闭")
    }
  }

  class ProducerCallback() extends Callback {
    override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
      if (e != null) {
        e.printStackTrace()
        System.err.println("发送失败....")
      } else {
        //        System.err.println("发送成功....")
      }
    }
  }

}
