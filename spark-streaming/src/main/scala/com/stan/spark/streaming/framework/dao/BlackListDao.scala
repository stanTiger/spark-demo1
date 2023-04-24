package com.stan.spark.streaming.framework.dao

import com.stan.spark.streaming.framework.util.{EnvUtil, MyKafkaUtil}
import com.stan.spark.streaming.framework.util.JdbcUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.collection.mutable.ListBuffer

class BlackListDao {
  def loadData(): InputDStream[ConsumerRecord[String, String]] = {
    val ssc: StreamingContext = EnvUtil.get()
    MyKafkaUtil.getKafkaStream("test", ssc)
  }

  def getBlackList(): List[String] = {
    val conn: Connection = JdbcUtil.getConnection
    val sql = "select userid from black_list"
    val list: List[String] = JdbcUtil.getDataFromMysql2(conn, sql, Array())
    conn.close()
    list
  }
}
