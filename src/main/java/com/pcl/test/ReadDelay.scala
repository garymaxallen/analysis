package com.pcl.test

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark

object ReadDelay {
  System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin-master")
  org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.WARN)
  private[this] val logger = Logger(this.getClass)


  def main(args: Array[String]): Unit = {

    //本地测试运行，需要开启下面两句代理，并且访问的是node5的32614端口，当上传到spark容器执行时，使用els容器本身的ip

    System.setProperty("http.proxyHost", "192.168.111.240");
    System.setProperty("http.proxyPort", "3128");

    val spark = SparkSession.builder()
      .appName("SparkWriteAndReadES")
      .master("local")
      .config("es.index.auto.create", "true")
      .config("es.nodes", "10.12.1.115")
      .config("es.port", "32614")
//      .config("es.nodes", "10.96.2.230")
//      .config("es.port", "9200")
      .config("es.nodes.wan.only", "true")
      .getOrCreate()

    val sc = spark.sparkContext

    val esQuery =
      """
        |{
        |  "query": {
        |    "bool":{
        |       "filter":[
        |           {
        |             "match":{
        |               "srcIP":"10.0.1.12"
        |             }
        |            },
        |            {
        |              "match":{
        |                 "protocolTYPE":"OSPF"
        |              }
        |            },
        |            {
        |               "range":{
        |                 "@timestamp":{
        |                   "gt":"2020-01-01T10:30:11.852Z"
        |                 }
        |               }
        |            }
        |           ]
        |     }
        |  },
        |  "size":100
        |}
      """.stripMargin

    val resRdd = EsSpark.esRDD(sc, "network_traffic_ela",esQuery)
    logger.warn("============开始：=================")
//    logger.warn("查询结果数量：" + resRdd.count())
    resRdd.foreach{case (_,result) =>{
      logger.warn(result.toString())
      val mapResult: collection.Map[String, AnyRef] = result
      logger.warn(mapResult.get("srcIP").toString)

    }}
    logger.warn("读取完毕")

    spark.stop()
  }

}
