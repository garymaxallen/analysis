package com.pcl.dataHandle

import com.pcl.test.DBUtils
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark


/**
 * Function:
 * Author: he
 * Date: 2019/12/17 17:26
 * Notice:通过源ip、目的ip、协议、源mac得到起始点发出的所有包，取结果中的第一个包（也即是时间最小的），得到ip_id、tcp_seq
 * 再通过源ip、目的ip、协议、目的mac、ip_id、tcp_seq得到上述那个包，到达目的点的该条数据，
 * 取两条数据的时间差值
 * 故需要已知，源ip、源mac、目的ip、目的mac
 * 协议如果不用tcp，则会出现tcp_seq为null，影响处理，故建议只依据tcp协议包来计算指标
 **/
object PTPDelay {
  // System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin-master")
  org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.INFO)
  private[this] val logger = Logger(this.getClass)


  var ip_id = ""
  var tcp_seq = ""
  var ip_src= "10.0.1.11"
  var ip_dst = "10.0.10.42"
  var eth_src = "02:00:5E:00:11:00"
  var eth_dst = "02:00:5E:00:42:00"
//  var protocols = "eth:ethertype:ip:icmp:data"
  var protocols = "eth:ethertype:ip:tcp"
  var time_epoch1 = "0"
  var time_epoch2 = "0"

  def main(args: Array[String]): Long = {

    //本地测试运行，需要开启下面两句代理，并且访问的是node5的32614端口，当上传到spark容器执行时，使用els容器本身的ip

    //    System.setProperty("http.proxyHost", "192.168.111.240");
    //    System.setProperty("http.proxyPort", "3128");

    val spark = SparkSession.builder()
      .appName("PTPDelay")
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

    val esQuery =
      s"""
         |{
         |  "query": {
         |    "bool":{
         |       "must":[
         |          {
         |             "match":{
         |               "eth_src":"${eth_src}"
         |             }
         |            },
         |           {
         |             "match":{
         |               "ip_src":"${ip_src}"
         |             }
         |            },
         |            {
         |             "match":{
         |               "protocols":"${protocols}"
         |             }
         |            },
         |            {
         |             "match":{
         |               "ip_dst":"${ip_dst}"
         |             }
         |            }
         |           ]
         |     }
         |  }
         |}
      """.stripMargin


    val resRdd = EsSpark.esRDD(sc, "network_traffic_ela", esQuery)
    resRdd.values.foreach(println)
    logger.warn("============开始：=================")
    val firstCount = resRdd.count()
    logger.warn("查询结果数量：" + resRdd.count())
    logger.warn("第一次读取完毕")
    if(firstCount > 0){
      logger.warn("取第一个数据包")
      resRdd.take(1).foreach { result => {
        val mapData = result._2
        logger.warn(mapData.toString())
        ip_id = mapData.get("ip_id").get.toString
        tcp_seq = mapData.get("tcp_seq").get.toString
        time_epoch1 = mapData.get("time_epoch").get.toString
        logger.warn(ip_id)
        logger.warn(tcp_seq)
        logger.warn(time_epoch1)
      }}

      val esQuery02 =
        s"""
           |{
           |  "query": {
           |    "bool":{
           |       "must":[
           |           {
           |             "match":{
           |               "ip_src":"${ip_src}"
           |             }
           |            },
           |            {
           |             "match":{
           |               "protocols":"${protocols}"
           |             }
           |            },
           |            {
           |             "match":{
           |               "ip_dst":"${ip_dst}"
           |             }
           |            },
           |            {
           |             "match":{
           |               "ip_id":"${ip_id}"
           |             }
           |            },
           |            {
           |             "match":{
           |               "eth_dst":"${eth_dst}"
           |             }
           |            },
           |            {
           |             "match":{
           |               "tcp_seq":"${tcp_seq}"
           |             }
           |            }
           |           ]
           |     }
           |  }
           |}
      """.stripMargin
      val resRddEnd = EsSpark.esRDD(sc, "network_traffic_ela", esQuery02)
      //    resRddEnd.values.foreach(println)
      logger.warn("============开始：=================")
      val rddEndCount = resRddEnd.count()
      logger.warn(s"查询结果数量：$rddEndCount")
      logger.warn("第二次读取完毕")

      resRddEnd.take(rddEndCount.toInt).foreach { result => {
        val mapData02 = result._2
        logger.warn(mapData02.toString())
        time_epoch2 = mapData02.get("time_epoch").get.toString
        logger.warn(time_epoch2)
      }}

      logger.warn("计算时间差")
      val delayMils = time_epoch2.replace(".","").toLong - time_epoch1.replace(".","").toLong
      logger.warn(s"时延为：$delayMils")
      spark.stop()

      saveToMysql(ip_src,ip_dst,delayMils.toString,ip_id,tcp_seq)
      delayMils
    }else{
      logger.warn("没有数据，不处理")
      0
    }
  }


  /**
   * Function: 时延计算结果入库
   * Author: he
   * Date: 2019/12/17 15:16
   * Notice:
   **/
  def saveToMysql(ipSrc:String,ipDst:String,delay:String,ipId:String,tcpSeq:String): Unit ={
    var mysqlConn = DBUtils.getConnection();

    try{
      //如果该包之前被计算过，本次不入库
      var statement = mysqlConn.createStatement()
      var selectResult = statement.executeQuery("select * from PTPDelay where tcp_seq=" + tcpSeq + " and ip_id=" + ipId)

      if (!selectResult.next()){
        //无记录时，新增记录
        //存入mysql
        val saveSql = "INSERT INTO PTPDelay(star_port,end_port,ip_id,tcp_seq,delay,create_at)VALUES(?,?,?,?,?,?)"
        var pstm = mysqlConn.prepareStatement(saveSql)
        pstm.setObject(1,ipSrc)
        pstm.setObject(2,ipDst)
        pstm.setObject(3,ipId)
        pstm.setObject(4,tcpSeq)
        pstm.setObject(5,delay)
        pstm.setObject(6,DBUtils.dateFormat("yyyy-MM-dd HH:mm:ss"))

        pstm.executeUpdate() > 0
      }

    }
    mysqlConn.close()
  }

}
