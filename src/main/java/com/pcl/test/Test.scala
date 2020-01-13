
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.MapWritable

object Test {

  def main(args: Array[String]): Unit = {

    val test = "1576489392.328093760"
    println(test)
    println(test.replace(".","").toLong - 1)

  }
}
