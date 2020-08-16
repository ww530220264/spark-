package com.ww.hbase

object Main {
  def main(args: Array[String]): Unit = {
    HbaseUtils.get("t1","111")
    println()
    // 删除指定列族的所有列
//    HbaseUtils.delete("t1","111",Seq("cf1").toArray,new Array[Array[String]](0))
    // 删除指定rowKey对应的行数据
//    HbaseUtils.delete("t1","111",new Array[String](0),new Array[Array[String]](0))
    println()
    HbaseUtils.scan("t1")
    println()
    HbaseUtils.closeConnection()
  }
}
