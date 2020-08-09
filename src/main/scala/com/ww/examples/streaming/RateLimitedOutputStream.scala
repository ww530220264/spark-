//package com.ww.examples.streaming
//
//import java.io.OutputStream
//import java.util.concurrent.TimeUnit
//import org.apache.spark.internal.Logging
//import scala.annotation.tailrec
//
//class RateLimitedOutputStream(out: OutputStream, desireBytesPerSec: Int) extends OutputStream with Logging {
//
//  require(desireBytesPerSec > 0)
//
//  private val SYNC_INTERVAL = TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS)
//  private val CHUNK_SIZE = 8192
//  private var lastSyncTime = System.nanoTime()
//  private var bytesWrittenSinceSync = 0L
//
//  override def write(b: Int): Unit = {
//    waitToWrite(1)
//    out.write(b)
//  }
//
//  override def write(bytes: Array[Byte]) {
//    write(bytes, 0, bytes.length)
//  }
//
//  @tailrec
//  override final def write(b: Array[Byte], off: Int, len: Int): Unit = {
//    val writeSize = math.min(len - off, CHUNK_SIZE)
//    if (writeSize > 0) {
//      waitToWrite(writeSize)
//      out.write(b, off, writeSize)
//      write(b, off + writeSize, len)
//    }
//  }
//
//  override def flush(): Unit = {
//    out.flush()
//  }
//
//  override def close(): Unit = {
//    out.close()
//  }
//
//  @tailrec
//  private def waitToWrite(numBytes: Int): Unit = {
//    val now = System.nanoTime()
//    val elapsedNanosecs = math.max(now - lastSyncTime, 1)
//    val rate = bytesWrittenSinceSync.toDouble * 1000000000 / elapsedNanosecs
//    if (rate < desireBytesPerSec) {
//      bytesWrittenSinceSync += numBytes
//      if (now > lastSyncTime + SYNC_INTERVAL) {
//        lastSyncTime = now
//        bytesWrittenSinceSync = numBytes
//      }
//    } else {
//      val targetTimeInMills = bytesWrittenSinceSync * 1000 / desireBytesPerSec
//      val elapsedTimeInMills = elapsedNanosecs / 1000000
//      val sleepTimeInMills = targetTimeInMills - elapsedTimeInMills
//      if (sleepTimeInMills > 0) {
//        logTrace("Natural rate is " + rate + " per second bug desired rate is " +
//          desireBytesPerSec + ", sleeeping for " + sleepTimeInMills + " ms to compensate.")
//        Thread.sleep(sleepTimeInMills)
//      }
//      waitToWrite(numBytes)
//    }
//  }
//}
