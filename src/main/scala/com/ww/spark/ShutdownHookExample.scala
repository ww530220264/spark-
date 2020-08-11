package com.ww.spark

// kill -9 pid不能捕获到
// kill 9 pid可以捕获
object ShutdownHookExample {
  def main(args: Array[String]): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        while (true){
          Thread.sleep(1111)
          System.err.println("111111")
        }
      }
    }).start()
    new Thread(new Runnable {
      override def run(): Unit = {
        while (true){
          Thread.sleep(1111)
          System.err.println("2222")
        }
      }
    }).start()

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        System.err.println("JVM关闭........")
      }
    }))
  }
}
