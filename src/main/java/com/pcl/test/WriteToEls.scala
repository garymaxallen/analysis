package com.pcl.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sql.EsSparkSQL

object WriteToEls {
  val logger =  Logger.getLogger("org")
  logger.setLevel(Level.ERROR)

  case class StuInfo(name: String, sex: String, age: Int)
  def main(args: Array[String]): Unit = {

    //本地测试运行，需要开启下面两句代理，并且访问的是node5的32614端口，当上传到spark容器执行时，使用els容器本身的ip

//    System.setProperty("http.proxyHost", "192.168.111.240");
//    System.setProperty("http.proxyPort", "3128");

    val spark = SparkSession.builder()
      .appName("SparkWriteAndReadES")
      .master("local")
      .config("es.index.auto.create", "true")
//      .config("es.nodes", "10.12.1.115")
//      .config("es.port", "32614")
      .config("es.nodes", "10.96.2.230")
      .config("es.port", "9200")
      .config("es.nodes.wan.only", "true")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd = sc.makeRDD(Seq(
      StuInfo("diaochan", "女", 34),
      StuInfo("dianwei", "男", 36),
      StuInfo("guojia", "男", 35)))
    val map = Map("es.mapping.id" -> "name")

    EsSpark.saveToEs(rdd, "spark/docs", map)
    System.out.println("============RDD写入ES成功！！！=================")
    val resRdd = EsSpark.esRDD(sc, "spark/docs")
    System.out.println("============用esRDD读取ES结果如下：=================")
    resRdd.foreach(println)


    val df = spark.createDataFrame(sc.parallelize(Seq(
      StuInfo("xiaoming", "男", 18),
      StuInfo("xiaohong", "女", 17),
      StuInfo("xiaozhao", "男", 19)))).toDF("name", "sex", "age")

    EsSparkSQL.saveToEs(df, "spark/docs", map)
    System.out.println("============RDD写入ES成功！！！=================")
    val esQuery =
      """
        |{
        |  "query": {
        |    "match": {
        |      "sex":"男"
        |    }
        |  }
        |}
      """.stripMargin
    val resDf = EsSparkSQL.esDF(spark, "spark/docs",esQuery)
    System.out.println("============用esDF读取ES结果如下：=================")
    resDf.orderBy("name").show(false)

    spark.stop()
  }

}
