package com.ww.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtils {

  System.setProperty("HADOOP_USER_NAME", "spark")

  def getSparkConf(appName: String): SparkConf = new SparkConf()
    .setAppName(appName)
    .setMaster("yarn")
    .set("yarn.resourcemanager.hostname", "cdh1")
    .set("spark.execurot.instance", "2")
    .set("spark.executor.cores","2")
    .set("spark.executor.memory", "512M")
    .set("spark.yarn.queue", "spark")
    .set("spark.driver.host", "192.168.10.107")
    .set("spark.yarn.archive", "hdfs://nameservice-HA-hdfs/user/spark/jars/spark-libs.jar")
    .setJars(List("C:\\workspace\\cdh\\target\\original-cdh-1.0-SNAPSHOT.jar"))


  def getSparkSession(sparkConf: SparkConf): SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  def setLogLevel(level: String): Unit = {

    val logLevel = level.toLowerCase() match {
      case "ERROR" => Level.ERROR
      case "WARN" => Level.WARN
      case "DEBUG" => Level.DEBUG
      case "INFO" => Level.INFO
      case _ => Level.ERROR
    }

    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      Logger.getRootLogger.setLevel(logLevel)
    }
  }
}

object SparkSessionSingleton {

  @transient private var instance: SparkSession = _

  def getInstannce(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession.builder().config(sparkConf).getOrCreate()
    }
    instance
  }
}
