package com.pcl.test


import java.io.IOException

import org.apache.http.client.config.RequestConfig
import org.apache.http.{HttpEntity, HttpHost, HttpResponse}
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark.esRDD

//https://blog.csdn.net/u010290051/article/details/86218410
object TestHttp {
  System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin-master")
  //  org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.WARN)
  System.setProperty("http.proxyHost", "192.168.111.240");
  System.setProperty("http.proxyPort", "3128");


  def main(args: Array[String]): Unit = {

    val esQuery=s"""
                   |{
                   |  "size":"1",
                   |  "query": {
                   |    "bool":{
                   |       "must":[
                   |           {
                   |             "match":{
                   |               "srcIP":"10.0.11.41"
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
                   |                   "gt":"2020-01-01T23:59:56.852Z"
                   |                 }
                   |               }
                   |            }
                   |           ]
                   |     }
                   |  }
                   |}
      """.stripMargin

    var response: String = postBody(
      url = s"http://10.12.1.115:32614/network_traffic_ela/_search?scroll=1m",
      body = esQuery,
      encoding = "utf-8")

    println(response)
  }


  def postBody(url: String, body: String, encoding: String): String = {
    val httpClient = HttpClients.createDefault
    var re: String = ""
    try {
      val httpPost: HttpPost = new HttpPost(url)

      val proxy = new HttpHost("192.168.111.240", 3128)
      val requestConfig = RequestConfig.custom().setProxy(proxy).build()
      httpPost.setConfig(requestConfig)

      httpPost.setEntity(new StringEntity(body, encoding))
      httpPost.addHeader("content-type","application/json")
      val httpResponse: HttpResponse = httpClient.execute(httpPost)
      val entity: HttpEntity = httpResponse.getEntity
      if (entity != null) {
        re = EntityUtils.toString(entity, encoding)
        println(re)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace(System.out)
    } finally try
      httpClient.close()
    catch {
      case e: IOException =>
        e.printStackTrace(System.out)
    }
    re
  }

  def test: Unit = {
    val httpUriRequest = new HttpGet("")
    httpUriRequest.setHeader("content-type","application/json")
  }
}
