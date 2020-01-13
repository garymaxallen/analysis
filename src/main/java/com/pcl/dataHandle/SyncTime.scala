package com.pcl.dataHandle
import com.pcl.test.DBUtils
import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.mutable.ListBuffer

object SyncTime {
  // System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin-master")
  org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.WARN)
  private[this] val logger = Logger(this.getClass)
  var list_1 = new ListBuffer[String]()
  var list_2 = new ListBuffer[String]()

  def main(args: Array[String]): Unit = {
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
    routerSyncTime(sc)
    spark.stop()
  }

  def routerSyncTime(sc:SparkContext): Unit = {
    val esQuery_1 =
      """
        |{
        | "query": {
        |   "bool":{
        |     "filter":[
        |       {
        |         "match":{
        |           "protocols":"eth:ethertype:ip:ospf"
        |          }
        |        },
        |        {
        |         "match":{
        |           "ospf_type":"2"
        |          }
        |        }
        |      ],
        |      "must_not":[
        |        {
        |         "match":{
        |           "protocols":"eth:ethertype:ip:tcp"
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}
      """.stripMargin

    val esQuery_2 =
      """
        |{
        | "query": {
        |   "bool":{
        |     "filter":[
        |       {
        |         "match":{
        |           "protocols":"eth:ethertype:ip:ospf"
        |          }
        |        },
        |        {
        |         "match":{
        |           "ospf_type":"5"
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}
      """.stripMargin
    val resRdd_1 = EsSpark.esRDD(sc, "network_traffic_ela",esQuery_1)
    val resRdd_2 = EsSpark.esRDD(sc, "network_traffic_ela",esQuery_2)
    logger.warn("============routerSyncTime 开始：=================")
    resRdd_1.foreach{
      result => {
        //logger.warn(result._2("time_epoch").toString)
        list_1 += result._2("time_epoch").toString
      }
    }
    resRdd_2.foreach{
      result => {
        //logger.warn(result._2("time_epoch").toString)
        list_2 += result._2("time_epoch").toString
      }
    }
    logger.warn("============routerSyncTime 读取完毕：=================")
    saveToMysql_syncTime(list_1.head, list_2.last, (list_2.last.toDouble - list_1.head.toDouble).toString)
  }

  def saveToMysql_syncTime(start_time:String, end_time:String, sync_time:String): Unit = {
    val mysqlConn = DBUtils.getConnection()

    try{
      //存入mysql
      val saveSql = "INSERT INTO SyncTime(start_time, end_time, sync_time, create_at)VALUES(?, ?, ?, ?)"
      val pstm = mysqlConn.prepareStatement(saveSql)
      pstm.setObject(1, start_time)
      pstm.setObject(2, end_time)
      pstm.setObject(3, sync_time)
      pstm.setObject(4, DBUtils.dateFormat("yyyy-MM-dd HH:mm:ss"))

      pstm.executeUpdate() > 0
    }
    mysqlConn.close()
  }
}
