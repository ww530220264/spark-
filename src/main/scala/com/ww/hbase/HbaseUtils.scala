package com.ww.hbase

import java.util
import java.util.concurrent.Executors
import java.util.function.Consumer

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CompareOperator, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, Connection, ConnectionFactory, Delete, Get, Put, Result, ResultScanner, Scan, TableDescriptorBuilder}
import org.apache.hadoop.hbase.filter.{BinaryComparator, ColumnValueFilter, PageFilter, RowFilter}
import org.apache.hadoop.hbase.util.Bytes

object HbaseUtils {

  val numThreads = Runtime.getRuntime.availableProcessors();
  val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("thread-pool-%d").build()
  val executors = Executors.newFixedThreadPool(numThreads, threadFactory)
  val threadLocalConnection = new ThreadLocal[Connection]()

  def getConf(): Configuration = {
    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "cdh0,cdh1,cdh2")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("zookeeper.znode.parent", "/hbase")
    // 更快的抛出异常,不用无限制的重试[测试用]
    hbaseConf.set("hbase.client.start.log.errors.counter", "1")
    hbaseConf.set("hbase.client.retries.number", "1")
    hbaseConf
  }

  private val connection: Connection = ConnectionFactory.createConnection(getConf())

  // 创建连接
  def getThreadConnection(): Connection = {
    if (threadLocalConnection.get() == null) {
      threadLocalConnection.set(ConnectionFactory.createConnection(getConf()))
    }
    threadLocalConnection.get()
  }

  // 关闭资源
  def closeConnection(): Unit = {
    if (connection != null) {
      connection.close()
    }
  }

  // 建表空间
  def createNamespace(namespace: String): Unit = {
    val admin = connection.getAdmin
    val namespaceDescriptor = admin.getNamespaceDescriptor(namespace)
    admin.createNamespace(namespaceDescriptor)
    admin.close()
  }

  // 删除表空间
  def deleteNamespace(namespace: String): Unit = {
    val admin = connection.getAdmin
    admin.deleteNamespace(namespace)
    admin.close()
  }

  // 建表
  def createTable(tableName: String, cfs: String*): Unit = {
    val table = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
    for (cf <- cfs) {
      table.setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf))
    }
    connection.getAdmin.createTable(table.build())
  }

  // 修改表
  // 删表
  def deleteTable(tableName: String): Unit = {
    connection.getAdmin.deleteTable(TableName.valueOf(tableName))
  }

  // 单条插入
  def insert(tableName: String, rowKey: String, cf: String, col: String, value: String): Unit = {
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col), Bytes.toBytes(value))
    val table = connection.getTable(TableName.valueOf(tableName))
    table.put(put)
    table.close()
  }

  // 多条插入
  def insertMultiColumns(tableName: String, rowKey: String, cfs: Array[String], clss: Array[Array[String]], valuess: Array[Array[String]]): Unit = {
    val put: Put = new Put(Bytes.toBytes(rowKey))
    for (i <- 0 until cfs.length) {
      val cf = cfs(i)
      val cls = clss(i)
      val values = valuess(i)
      for (j <- 0 until cls.length) {
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cls(j)), Bytes.toBytes(values(j)))
      }
    }
    val table = connection.getTable(TableName.valueOf(tableName))
    table.put(put)
    table.close()
  }

  // 删除
  def delete(tableName: String, rowKey: String, cfs: Array[String], clss: Array[Array[String]]): Unit = {
    val delete = new Delete(Bytes.toBytes(rowKey))
    for (i <- 0 until cfs.length) {
      val cf = cfs(i)
      if (clss.length > 0) {
        val cls = clss(i)
        for (j <- 0 until cls.length) {
          delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cls(j)))
        }
      } else {
        delete.addFamily(Bytes.toBytes(cf))
      }
    }
    val table = connection.getTable(TableName.valueOf(tableName))
    table.delete(delete)
    table.close()
  }

  // 查询
  def get(tableName: String, rowKey: String): Unit = {
    val get = new Get(Bytes.toBytes(rowKey))
    get.readAllVersions()
    val table = connection.getTable(TableName.valueOf(tableName))
    val result = table.get(get)
    val stringRess = resultToString(result)
    stringRess.forEach(new Consumer[String] {
      override def accept(t: String): Unit = {
        System.err.println(t)
      }
    })
  }

  // resultToStrings
  def resultToString(result: Result): util.ArrayList[String] = {
    val cells = result.rawCells()
    val stringRes = new util.ArrayList[String]()
    for (cell <- cells) {
      val rowKey = new String(cell.getRowArray, cell.getRowOffset, cell.getRowLength)
      val cf = new String(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength)
      val col = new String(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
      val value = new String(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
      val stringRe = s"""{"rowKey":"${rowKey}","cf":"${cf}","col":"${col}","value":"${value}","timestamp":"${cell.getTimestamp}"}"""
      stringRes.add(stringRe)
    }
    stringRes
  }

  // 扫描
  def scan(tableName: String): Unit = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val scan = new Scan
//    scan.setFilter(new RowFilter(CompareOperator.GREATER,new BinaryComparator(Bytes.toBytes("111"))))
//    scan.setFilter(new ColumnValueFilter(Bytes.toBytes("cf1"),Bytes.toBytes("name"),CompareOperator.NOT_EQUAL,Bytes.toBytes("wangwei")))
    scan.withStartRow(Bytes.toBytes("111")) // >=
    scan.withStopRow(Bytes.toBytes("333"))  // <
    scan.setCacheBlocks(false) // 扫描过程不使用block cache
    val scanner = table.getScanner(scan)
    val iterator = scanner.iterator()
    while (iterator.hasNext) {
      val result = iterator.next()
      val strings = resultToString(result)
      strings.toArray().foreach(println)
    }
    scanner.close()
  }
}
