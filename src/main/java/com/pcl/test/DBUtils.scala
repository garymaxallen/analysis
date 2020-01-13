package com.pcl.test

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Date


/**
  * Function: 连接mysql工具类
  * Author: he
  * Date: 2018-07-09 15:49
  * Notice:
  **/
object DBUtils {
  val IP = "192.168.67.240"
  val password = "qazWSX123#"
  val username = "root"
  val Port = "2292"
  val DBType = "mysql"
  val DBName = "communication"
  val url = "jdbc:" + DBType + "://" + IP + ":" + Port + "/" + DBName + "?characterEncoding=UTF8"
  classOf[com.mysql.jdbc.Driver]

  def getConnection(): Connection = {
    DriverManager.getConnection(url, username, password)
  }

  def close(conn: Connection): Unit = {
    try {
      if (!conn.isClosed() || conn != null) {
        conn.close()
      }
    }
    catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }

  def dateFormat(format:String): String = {
    val now: Date = new Date()
//    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateFormat: SimpleDateFormat = new SimpleDateFormat(format)
    val date = dateFormat.format(now)
    date
  }
}
