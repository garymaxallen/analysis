package com.pcl.dataHandle

import com.pcl.dataHandle.PortThroughput.{lastDataTime, totalLength}
import com.pcl.test.DBUtils
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark

object LinkThroughput {
  // System.setProperty("hadoop.home.dir", "/home/daniel/Downloads/hadoop-3.2.1")
  org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.INFO)
  private[this] val logger = Logger(this.getClass)


  var ip_src= "10.0.9.32"
  var ip_dst = "10.0.10.42"
  var eth_src = "8e:b2:6f:23:4a:c1"
  var eth_dst = "02:e0:28:be:cc:5c"
  //  var protocols = "eth:ethertype:ip:icmp:data"
  var protocols = "eth:ethertype:ip:tcp:data"
  var timeLength = 500

  // Start time
  var time_epoch1 = (System.currentTimeMillis()-timeLength)/1000

  // End time
  var time_epoch2 = System.currentTimeMillis()/1000

  def main(args: Array[String]): Unit = {

    //本地测试运行，需要开启下面两句代理，并且访问的是node5的32614端口，当上传到spark容器执行时，使用els容器本身的ip

    //    System.setProperty("http.proxyHost", "192.168.111.240");
    //    System.setProperty("http.proxyPort", "3128");

    val spark = SparkSession.builder()
      .appName("LinkThroughput")
      .master("local")
      .config("es.index.auto.create", "true")
      .config("es.nodes", "192.168.67.240")
      .config("es.port", "32614")
      //      .config("es.nodes", "10.96.2.230")
      //      .config("es.port", "9200")
      .config("es.nodes.wan.only", "true")
      .config("es.index.read.missing.as.empty", "true")
      //        .config("es.scroll.limit", "1")
      .getOrCreate()

    val sc = spark.sparkContext

    // Time frame missing

    val esQuery =
      s"""
         |{
         |  "query": {
         |    "bool":{
         |       "must":[
         |           {
         |             "match":{
         |               "eth_dst":"${eth_dst}"
         |             }
         |            },
         |            {
         |             "match":{
         |               "ip_src":"${ip_src}"
         |             }
         |            },
         |            {
         |             "match":{
         |               "ip_dst":"${ip_dst}"
         |             }
         |            }
         |            {
         |             "match":{
         |               "eth_src":"${eth_src}"
         |             }
         |            }
         |           ],
         |        "filter":[
         |              {
         |                "range":{
         |                  "time_epoch":{
         |                    "gt":"${time_epoch1}",
         |                    "lt":"${time_epoch2}"
         |                   }
         |                 }
         |             }
         |           ]
         |     }
         |  }
         |}
      """.stripMargin


    val resRdd = EsSpark.esRDD(sc, "network_traffic_ela", esQuery)
    resRdd.values.foreach(println)
    logger.warn("============开始：=================")
    val packageCount = resRdd.count()
    logger.warn("数据包个数：" + resRdd.count())
    logger.warn("第一次读取完毕")
    if(packageCount > 0){

      resRdd.foreach { result => {
        val mapData = result._2
        lastDataTime = mapData.get("time_epoch").get.toString
        totalLength = totalLength + mapData.get("length").get.toString.toLong
        logger.warn(mapData.toString())
      }}
      logger.warn("============开始：=================")
      logger.warn(s"数据总长度：$totalLength")

      logger.warn("计算端到端链路吞吐量")
      val linkThroughput = totalLength/(timeLength/1000)
      logger.warn(s"时延为：$linkThroughput")
      spark.stop()

      saveToMysql(eth_dst,ip_src,ip_dst,time_epoch1.toString,time_epoch2.toString,totalLength.toString,linkThroughput.toString)
    }else{
      logger.warn("没有数据，不处理")
    }
  }


  /**
   * Function: 端到端链路吞吐量入库
   * Author: Zeyao
   * Date: 2019/12/24 15:43
   **/
  def saveToMysql(eth_dst:String,ip_src:String,ip_dst:String,start_time:String,end_time:String, total_lenght:String, throughput:String): Unit ={
    var mysqlConn = DBUtils.getConnection();

    try{

      //存入mysql
      val saveSql = "INSERT INTO Throughput(type,eth_dst,ip_src,ip_dst,start_time,end_time,total_length,throughput,create_at)" +
                    "VALUES(?,?,?,?,?,?,?,?,?)"
      var pstm = mysqlConn.prepareStatement(saveSql)
      pstm.setObject(1,"link")
      pstm.setObject(2,eth_dst)
      pstm.setObject(3,ip_src)
      pstm.setObject(4,ip_dst)
      pstm.setObject(5,start_time)
      pstm.setObject(6,end_time)
      pstm.setObject(7,total_lenght)
      pstm.setObject(8,throughput)
      pstm.setObject(9,DBUtils.dateFormat("yyyy-MM-dd HH:mm:ss"))

      pstm.executeUpdate() > 0
    }
    mysqlConn.close()
  }
}
