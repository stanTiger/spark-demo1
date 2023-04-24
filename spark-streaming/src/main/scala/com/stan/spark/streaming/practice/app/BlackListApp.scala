package com.stan.spark.streaming.practice.app

import com.stan.spark.streaming.practice.bean.Ads_log
import com.stan.spark.streaming.practice.util.{JdbcUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object BlackListApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BlackListApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    // 读取 kafka 数据
    val kvDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("test", ssc)
    val datas: DStream[String] = kvDS.map(_.value())
    val adsLogDS: DStream[Ads_log] = datas.map(
      line => {
        val fields: Array[String] = line.split(" ")
        Ads_log(fields(0).toLong, fields(1), fields(2), fields(3), fields(4))
      }
    )
    adsLogDS.cache()


    val clickCountDS: DStream[((String, String, String), Int)] = adsLogDS.transform(
      rdd => {
        val conn: Connection = JdbcUtil.getConnection
        val sql: String = "select userid from black_list"
        val pstmt: PreparedStatement = conn.prepareStatement(sql)
        val resultSet: ResultSet = pstmt.executeQuery()

        val list: ListBuffer[String] = ListBuffer[String]()
        while (resultSet.next()) {
          list.append(resultSet.getString(1))
        }
        resultSet.close()
        pstmt.close()
        conn.close()
        //        println(list.mkString(","))
        // 过滤黑名单
        val filterRDD: RDD[Ads_log] = rdd.filter(data => !list.contains(data.userid))

        // 计算 ((dt,userid,ad),cnt)
        val adsClickRDD: RDD[((String, String, String), Int)] = filterRDD.map(
          data => {
            val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
            val dt: String = sdf.format(new Date(data.timestamp))
            val userid: String = data.userid
            val ad: String = data.adid
            ((dt, userid, ad), 1)
          }
        ).reduceByKey(_ + _)

        // 单次点击数 > 100 拉入黑名单
        // 单次 < 100 更新点击表
        adsClickRDD
      }
    )

    clickCountDS.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            val conn: Connection = JdbcUtil.getConnection
            iter.foreach {
              case ((dt, useid, ad), count) => {
                // 单次点击数 > 100 拉入黑名单
                if (count >= 100) {
                  val sql: String = "insert into black_list(userid) values(?) on duplicate key update userid = ?"
                  JdbcUtil.executeUpdate(conn, sql, Array(useid, useid))
                } else { // 单次 < 100 更新点击表
                  val sql: String =
                    """
                      |Insert into user_ad_count(dt,userid,adid,count)
                      |values (?,?,?,?)
                      |on duplicate key
                      |update count=count+?
                    """.stripMargin
                  JdbcUtil.executeUpdate(conn, sql, Array(dt, useid, ad, count, count))

                  // 获取数据查看是否大于100
                  val sql2: String =
                    """
                      |select count
                      |from user_ad_count
                      |where dt = ? and userid = ? and adid = ? and count >= 100
                      |""".stripMargin
                  val flg: Boolean = JdbcUtil.isExist(conn, sql2, Array(dt, useid, ad))
                  // 如果超过插入 black_list
                  if (flg) {
                    val sql3: String = "insert into black_list(userid) values(?) on duplicate key update userid = ?"
                    JdbcUtil.executeUpdate(conn, sql3, Array(useid, useid))
                  }
                }
              }
            }
            conn.close()
          }
        )
      }
    )

    // 实时统计 广告点击量  ((dt,area,city,adid),count)
    val adClickCountDS: DStream[((String, String, String, String), Int)] = adsLogDS.map(
      data => {
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val dt: String = sdf.format(new Date(data.timestamp))
        val area: String = data.area
        val city: String = data.city
        val adid: String = data.adid
        ((dt, area, city, adid), 1)
      }
    ).reduceByKey(_ + _)
    adClickCountDS.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iterm => {
            val conn: Connection = JdbcUtil.getConnection
            iterm.foreach {
              case ((dt, area, city, adid), count) => {
                val sql: String =
                  """
                    |INSERT INTO area_city_ad_count(dt,area,city,adid,cnt)
                    |VALUES(?,?,?,?,?)
                    |ON DUPLICATE KEY
                    |UPDATE cnt=cnt+?
                  """.stripMargin
                JdbcUtil.executeUpdate(conn, sql, Array(dt, area, city, adid, count, count))
              }
            }
            conn.close()
          }
        )
      }
    )

    // 最近一小时广告点击量   ((adid,time),count)
    val adTimeCountDS: DStream[((String, String), Int)] = adsLogDS.map(
      adsLog => {
        val sdf: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
        val newTs: Long = adsLog.timestamp / 10000 * 10000
        val time: String = sdf.format(new Date(newTs))
        val adid: String = adsLog.adid
        ((adid, time), 1)
      }
    ).reduceByKeyAndWindow(
      (x: Int, y: Int) => x + y,
      Seconds(60),
      Seconds(10)
    )

    val adTimeSortDS: DStream[(String, List[(String, Int)])] = adTimeCountDS.map {
      case ((adid, time), count) => (adid, (time, count))
    }
      .groupByKey()
      .mapValues(
        iterm => {
          iterm.toList.sortBy(_._1)
        }
      )

    adTimeSortDS.print()






    // 开启采集
    ssc.start()
    ssc.awaitTermination()
  }

}
