//package com.ww.examples.streaming
//
//import java.io.{ByteArrayOutputStream, IOException}
//import java.net.ServerSocket
//import java.nio.ByteBuffer
//
//import org.apache.spark.SparkConf
//import org.apache.spark.serializer.KryoSerializer
//
//import scala.io.Source
//
//object RawTextSender {
//  def main(args: Array[String]): Unit = {
//    if (args.length < 4) {
//      System.err.println("Usage: RawTextSender <port> <file> <blockSize> <bytesPerLimit>")
//      System.exit(1)
//    }
//
//    val port = args(0).toInt
//    val file = args(1)
//    val blockSize = args(2).toInt
//    val bytesPerSec = args(3).toInt
//
//    val lines = Source.fromFile(file).getLines().toArray
//    val bufferStream = new ByteArrayOutputStream(blockSize + 1000)
//    val ser = new KryoSerializer(new SparkConf()).newInstance()
//    val serStream = ser.serializeStream(bufferStream)
//    var i = 0;
//    while (bufferStream.size() < blockSize) {
//      serStream.writeObject(lines(i))
//      i = (i + 1) % lines.length
//    }
//
//    val array = bufferStream.toByteArray
//
//    val countBuf = ByteBuffer.wrap(new Array[Byte](4))
//    countBuf.putInt(array.length)
//    countBuf.flip()
//
//    val serverSocket = new ServerSocket(port)
//
//    while (true) {
//      val socket = serverSocket.accept()
//      val out = new RateLimitedOutputStream(socket.getOutputStream, bytesPerSec)
//      try {
//        while (true) {
//          out.write(countBuf.array())
//          out.write(array)
//        }
//      } catch {
//        case e: IOException => System.err.println("Client disconnected...")
//      } finally {
//        socket.close()
//      }
//    }
//  }
//}
