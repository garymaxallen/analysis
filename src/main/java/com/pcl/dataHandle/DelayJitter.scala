package com.pcl.dataHandle

import com.pcl.test.DBUtils
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark


/**
 * Function:
 * Author: he
 * Date: 2019/12/17 17:26
 * Notice:取相邻两次端到端时延作差
 **/
object DelayJitter {
  // System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin-master")
  org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.INFO)
  private[this] val logger = Logger(this.getClass)

  var delay_value1 = 0L
  var delay_value2 = 0L

  def main(args: Array[String]): Unit = {

    var mysqlConn = DBUtils.getConnection();
    var handleId = 0

    var statement = mysqlConn.createStatement()
    var selectResult = statement.executeQuery("select * from delayJitter order by id DESC limit 1")
    while (selectResult.next()) {
      handleId = selectResult.getString("handle_id").toInt
      logger.warn(s"当前处理id= " + handleId)
    }

    statement = mysqlConn.createStatement()
    selectResult = statement.executeQuery("select * from PTPDelay where id =" + (handleId+2))
    while (selectResult.next()){
      delay_value1 = selectResult.getString("delay").toLong

      val statement2 = mysqlConn.createStatement()
      val selectResult2 = statement2.executeQuery("select * from PTPDelay where id =" + (handleId + 1))
      while (selectResult2.next()) {
        delay_value2 = selectResult2.getString("delay").toLong
      }
    }

    if (delay_value1 > 0) {
      val jitter = delay_value1 - delay_value2

      logger.warn("此次计算的时延抖动为： " + jitter)

      //入库
      saveToMysql(jitter.toString,(handleId+2).toString)
    }


  }


  /**
   * Function: 时延抖动计算结果入库
   * Author: he
   * Date: 2019/12/17 15:16
   * Notice:
   **/
  def saveToMysql(jitter:String,handleId:String): Unit ={
    var mysqlConn = DBUtils.getConnection();

    try{
      //存入mysql
      val saveSql = "INSERT INTO delayJitter(jitter,handle_id,create_at)VALUES(?,?,?)"
      var pstm = mysqlConn.prepareStatement(saveSql)
      pstm.setObject(1,jitter)
      pstm.setObject(2,handleId)
      pstm.setObject(3,DBUtils.dateFormat("yyyy-MM-dd HH:mm:ss"))

      pstm.executeUpdate() > 0
    }
    mysqlConn.close()
  }

}
