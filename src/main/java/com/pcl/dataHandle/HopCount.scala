package com.pcl.dataHandle

import com.pcl.test.DBUtils
import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.mutable.ListBuffer

object HopCount {
  // System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin-master")
  org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.WARN)
  private[this] val logger = Logger(this.getClass)

  def main(args: Array[String]) : Unit = {
    val spark = SparkSession.builder()
      .appName("HopCount")
      .master("local")
      .config("es.index.auto.create", "true")
      .config("es.nodes", "192.168.67.240")
      .config("es.port", "32614")
      .config("es.nodes.wan.only", "true")
      //.config("es.scroll.limit", 10)
      .getOrCreate()
    val sc = spark.sparkContext
    hopCount(sc)
    spark.stop()
  }

  def hopCount(sc:SparkContext): Unit = {
    val esQuery =
      """
        |{
        | "query": {
        |   "bool":{
        |     "filter":[
        |       {
        |         "match":{
        |           "protocols":"eth:ethertype:ip:tcp"
        |          }
        |        },
        |        {
        |         "match":{
        |           "ip_src":"10.0.1.11"
        |          }
        |        },
        |        {
        |         "match":{
        |           "ip_dst":"10.0.10.42"
        |          }
        |        },
        |        {
        |         "match":{
        |           "eth_src":"02:00:5E:00:11:00"
        |          }
        |        },
        |        {
        |         "match":{
        |           "eth_dst":"02:00:5E:00:42:00"
        |          }
        |        },
        |        {
        |         "match":{
        |           "ip_id":"0x0000a65d"
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}
      """.stripMargin

    val resRdd = EsSpark.esRDD(sc, "network_traffic_ela", esQuery)
    logger.warn("============HopCount 开始：=================")
    resRdd.foreach{ case (_,result) =>
      //logger.warn(result.toString())
    }
    System.out.println(resRdd.count())
    logger.warn("============HopCount 读取完毕：=================")
    saveToMysql_hopCount("10.0.1.11", "10.0.10.42", "299", "0x00001f55", resRdd.count()/2)
  }

  def saveToMysql_hopCount(ip_src:String, ip_dst:String, tcp_seq:String, ip_id:String, hopcount:Long): Unit = {
    val mysqlConn = DBUtils.getConnection()

    try{
      //存入mysql
      val saveSql = "INSERT INTO HopCount(ip_src, ip_dst, tcp_seq, ip_id, hopcount, create_at)VALUES(?, ?, ?, ?, ?, ?)"
      val pstm = mysqlConn.prepareStatement(saveSql)
      pstm.setObject(1, ip_src)
      pstm.setObject(2, ip_dst)
      pstm.setObject(3, tcp_seq)
      pstm.setObject(4, ip_id)
      pstm.setObject(5, hopcount)
      pstm.setObject(6, DBUtils.dateFormat("yyyy-MM-dd HH:mm:ss"))

      pstm.executeUpdate() > 0
    }
    mysqlConn.close()
  }
}
