package com.pcl.dataHandle

import com.pcl.test.DBUtils
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark


/**
 * Function:
 * Author: he
 * Date: 2019/12/17 17:26
 * Notice:丢包率是指一段时间内，端到端链路上，目的节点未收到数据包总数和源节点发送的数据包总数的比值。以tcp为例，同时为排除ack、syn等校验信息对丢包率计算的影响，在进行数据包记录查询时设置frame.protocols=eth:ethertype:ip:tcp:data。
 * （1）通过对<eth.src, ip.src, ip.dst, frame.time_epoch, frame.protocols>的查询，获取源节点向目的节点发送的所有数据包，个数为x；
 * （2）通过对<eth.dst, ip.src, ip.dst, frame.time_epoch, frame.protocols>的查询，获取目的节点接收到的所有数据包，个数为y。
 * （3）注意这里的frame.time_epoch采用一个时间区间来查询，若源节点查询的时间区间为<t1,t2>，此时间段计算的端到端链路时延为T，则目的节点处的查询时间区间为<t1+T,t2+T>。该时间区间应该在一个较小的时间间隔内选取，避免时间间隔较大后，端到端时延变化较大，统计的数据包个数存在较大差异的问题；
 * （4）则该端到端链路在这一时间段内的丢包率为R(packet loss rate)=(x-y)/x。
 **/
object PackageLoss {
  // System.setProperty("hadoop.home.dir", "/home/daniel/Downloads/hadoop-3.2.1")
  org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.INFO)
  private[this] val logger = Logger(this.getClass)


  var ip_src= "10.0.1.11"
  var ip_dst = "10.0.10.42"
  var eth_src = "02:00:5E:00:11:00"
  var eth_dst = "02:00:5E:00:42:00"
  //  var protocols = "eth:ethertype:ip:icmp:data"
  var protocols = "eth:ethertype:ip:tcp:data"

  // Start time
  var time_epoch1 = System.currentTimeMillis()/1000 -1

  // End time
  var time_epoch2 = time_epoch1 + 1

  def main(args: Array[String]): Unit = {

    //本地测试运行，需要开启下面两句代理，并且访问的是node5的32614端口，当上传到spark容器执行时，使用els容器本身的ip

    //    System.setProperty("http.proxyHost", "192.168.111.240");
    //    System.setProperty("http.proxyPort", "3128");

    val spark = SparkSession.builder()
      .appName("PackageLoss")
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
         |         ]
         |     }
         |  }
         |}
      """.stripMargin


    val resRdd = EsSpark.esRDD(sc, "network_traffic_ela", esQuery)
    resRdd.values.foreach(println)
    logger.warn("============开始：=================")
    val startPackageCount = resRdd.count()
    logger.warn("源节点包计数：" + resRdd.count())
    logger.warn("第一次读取完毕")
    if(startPackageCount > 0){

      var delay = PTPDelay.main(new Array[String](0))
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
           |            }
           |            {
           |             "match":{
           |               "eth_dst":"${eth_dst}"
           |             }
           |            }
           |           ],
           |         "filter":[
           |              {
           |                "range":{
           |                  "time_epoch":{
           |                    "gt":"${(time_epoch1 + delay/1000).toString}",
           |                    "lt":"${(time_epoch2 + delay/1000).toString}"
           |                   }
           |                 }
           |             }
           |         ]
           |     }
           |  }
           |}
        """.stripMargin
        val resRddEnd = EsSpark.esRDD(sc, "network_traffic_ela", esQuery02)
        //    resRddEnd.values.foreach(println)
        logger.warn("============开始：=================")
        val endPackageCount = resRddEnd.count()
        logger.warn(s"目标节点包个数：$endPackageCount")
        logger.warn("第二次读取完毕")


        logger.warn("计算丢包率")
        val packageLoss = (startPackageCount - endPackageCount)/startPackageCount
        logger.warn(s"丢包率：$packageLoss")
        spark.stop()

        saveToMysql(ip_src,ip_dst,time_epoch1.toString,time_epoch2.toString,packageLoss.toString)
    }else{
      logger.warn("没有数据，不处理")
    }
  }


  /**
   * Function: 丢包率计算结果入库
   * Author: he
   * Date: 2019/12/17 15:16
   * Notice:
   **/
  def saveToMysql(ipSrc:String,ipDst:String,startTime:String,endTime:String,packageLoss:String): Unit ={
    var mysqlConn = DBUtils.getConnection();

    try{
      //如果改包之前被计算过，本次不入库
      var statement = mysqlConn.createStatement()
      var selectResult = statement.executeQuery("select * from PackageLoss where start_port=" + ipSrc
        + " and end_port=" + ipDst + " and start_time="+time_epoch1 + " and end_time=" +time_epoch2)

      //存入mysql
      if(!selectResult.next()) {
        val saveSql = "INSERT INTO PackageLoss(start_port,end_port,start_time,end_time,package_loss,create_at)VALUES(?,?,?,?,?,?)"
        var pstm = mysqlConn.prepareStatement(saveSql)
        pstm.setObject(1, ipSrc)
        pstm.setObject(2, ipDst)
        pstm.setObject(3, startTime)
        pstm.setObject(4, endTime)
        pstm.setObject(5, packageLoss)
        pstm.setObject(6, DBUtils.dateFormat("yyyy-MM-dd HH:mm:ss"))

        pstm.executeUpdate() > 0
      }
    }
    mysqlConn.close()
  }

}
