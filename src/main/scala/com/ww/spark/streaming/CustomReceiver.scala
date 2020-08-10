package com.ww.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import com.ww.spark.SparkUtils
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

object CustomReceiver {
  def main(args: Array[String]): Unit = {
    val sparkConf = SparkUtils.getSparkConf("ww-CustomReceiver")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.sparkContext.setLogLevel("ERROR")
    val receivedStream = ssc.receiverStream(new CustomReceiver("192.168.10.161", 9999))
    receivedStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

class CustomReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {
  override def onStart(): Unit = {
    new Thread("Socket Receiver") {

      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  private def receive(): Unit = {
    var socket: Socket = null
    var userInput: String = null

    try {
      logInfo(s"Connecting to $host $port")
      socket = new Socket(host, port)
      logInfo(s"Connected to $host $port")
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
      userInput = reader.readLine()
      while (!isStopped() && userInput != null) {
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()
      logInfo("Stopping receiving")
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException => restart(s"Error connecting to $host $port")
      case t: Throwable => restart("Error receiving data", t)
    }
  }

  override def onStop(): Unit = {

  }
}
