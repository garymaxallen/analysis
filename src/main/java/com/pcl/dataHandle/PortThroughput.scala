package com.pcl.dataHandle

import com.pcl.dataHandle.LinkThroughput.time_epoch2
import com.pcl.test.DBUtils
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark


/**
 * Function:统计系统端口吞吐量
 * Author: he
 * Date: 2019/12/20 16:25
 * Notice:以leo11的mac作为查询条件，在一个10s的时间区间中，搜索所有leo11相连端口发送的数据包，取总数据长度，除以时间
 **/
object PortThroughput {
  // System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin-master")
  org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.INFO)
  private[this] val logger = Logger(this.getClass)


  var le_time = ""
  var gt_time = ""
  var eth_src = "02:00:5E:00:11:00"

  var totalLength = 0L

  var timelength = 500
  //每次计算完将最终的时间入库，如果仿真停止了，此处时间相同，可以避免重复入库
  var lastDataTime = "0"

  def main(args: Array[String]): Unit = {

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

    //TODO need test 1s
    val currentTime01 = System.currentTimeMillis().toString
    val currentTime02 = (System.currentTimeMillis() - timelength).toString

    le_time = currentTime01.substring(0,10) + "." + currentTime01.substring(10,13) + "000000"
    gt_time = currentTime02.substring(0,10) + "." + currentTime02.substring(10,13) + "000000"

    val esQuery =
      s"""
         |{
         |  "query": {
         |    "bool":{
         |       "must":[
         |       {
         |           "match":{
         |               "eth_src":"${eth_src}"
         |             }
         |       }
         |       ],
         |       "filter":[
         |         {
         |          "range":{
         |            "time_epoch":{
         |              "gt":"${gt_time}",
         |              "lt":"${le_time}"
         |            }
         |          }
         |        }
         |       ]
         |     }
         |  }
         |}
      """.stripMargin


    val resRdd = EsSpark.esRDD(sc, "network_traffic_ela", esQuery)
//    resRdd.values.foreach(println)
    logger.warn("============开始：=================")
    val firstCount = resRdd.count()
    logger.warn("查询结果数量：" + resRdd.count())
    logger.warn("第一次读取完毕")
    if(firstCount > 0){
      resRdd.foreach { result => {
        val mapData = result._2
        lastDataTime = mapData.get("time_epoch").get.toString
        totalLength = totalLength + mapData.get("length").get.toString.toLong
        logger.warn(mapData.toString())
      }}
      var portThroughput = totalLength/(timelength/1000)
      logger.warn("计算得到1秒内的总数据长度为：" + portThroughput)
      spark.stop()

      saveToMysql(portThroughput.toString,lastDataTime)
    }else{
      logger.warn("没有数据，不处理")
    }
  }


  /**
   * Function: 时延计算结果入库
   * Author: he
   * Date: 2019/12/17 15:16
   * Notice:
   **/
  def saveToMysql(through:String,lastTime:String): Unit ={
    var mysqlConn = DBUtils.getConnection();

    try{
      //如果改包之前被计算过，本次不入库
      var statement = mysqlConn.createStatement()
      var selectResult = statement.executeQuery("select * from portThrough where through=" + through + " and last_time=" + lastTime)
      if(!selectResult.next()){

        //存入mysql
        val saveSql = "INSERT INTO portThrough(through,last_time,create_at)VALUES(?,?,?)"
        var pstm = mysqlConn.prepareStatement(saveSql)
        pstm.setObject(1,through)
        pstm.setObject(2,lastTime)
        pstm.setObject(3,DBUtils.dateFormat("yyyy-MM-dd HH:mm:ss"))

        pstm.executeUpdate() > 0
      }

    }
    mysqlConn.close()
  }

}
