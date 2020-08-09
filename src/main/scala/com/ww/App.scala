package com.ww

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Hello world!
 *
 */
object App {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "spark")
    val conf = new SparkConf()
      .setAppName("WordCount")
      // 设置yarn-client模式提交
      .setMaster("yarn")
//      .set("deploy-mode","cluster")
      // 设置resourcemanager的ip
      .set("yarn.resourcemanager.hostname", "cdh1")
      // 设置executor的个数
      .set("spark.executor.instance", "2")
      // 设置executor的内存大小
      .set("spark.executor.memory", "512M")
      // 设置提交任务的yarn队列
      .set("spark.yarn.queue", "spark")
      .set("spark.yarn.archive","hdfs://nameservice-HA-hdfs/user/spark/jars/spark-libs.jar")
      // 设置driver的ip地址
      .set("spark.driver.host", "192.168.10.106")
      // 设置jar包的路径,如果有其他的依赖包,可以在这里添加,逗号隔开
      .setJars(List("C:\\ww\\cdh\\target\\original-cdh-1.0-SNAPSHOT.jar"
      ))
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val lines = sc.parallelize(List("aa,bb,cc,dd,ee,aa,dd,ff,gg,ss,ee,cc,ss"))
    val words = lines.flatMap(_.split(","))
    val res = words.map((_, 1)).reduceByKey(_ + _)
    System.err.println("+++++++++++++++++++++++++++++++++++" + res.collect().mkString(","))
  }
}
