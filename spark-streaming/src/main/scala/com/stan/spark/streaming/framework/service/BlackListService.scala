package com.stan.spark.streaming.framework.service

import com.stan.spark.streaming.framework.bean.Ads_log
import com.stan.spark.streaming.framework.dao.BlackListDao
import com.stan.spark.streaming.practice.util.JdbcUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

class BlackListService {
  private val blackListDao = new BlackListDao()

  def analysisData() = {
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = blackListDao.loadData()

    val adsLogDS: DStream[Ads_log] = kafkaDS.map(
      kafkaData => {
        val fields: Array[String] = kafkaData.value().split(" ")
        val ts: Long = fields(0).toLong
        val area: String = fields(1)
        val city: String = fields(2)
        val userid: String = fields(3)
        val ad: String = fields(4)
        Ads_log(ts, area, city, userid, ad)
      }
    )

    // 过滤黑名单数据
    val filterAdsLogDS: DStream[Ads_log] = adsLogDS.transform(
      rdd => {
        // 取出黑名单
        val blackList: List[String] = blackListDao.getBlackList()
        rdd.filter(
          adLog => !blackList.contains(adLog.userid)
        )
      }
    )

    // ((dt,ad,userid),count)
    val dtAdUseridToCountDS: DStream[((String, String, String), Int)] = filterAdsLogDS.map(
      adLog => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val dt: String = sdf.format(new Date(adLog.ts))
        val ad: String = adLog.ad
        val userid: String = adLog.userid
        ((dt, ad, userid), 1)
      }
    ).reduceByKey(_ + _)

    //process data
    dtAdUseridToCountDS.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            val conn: Connection = JdbcUtil.getConnection
            iter.foreach {
              case ((dt, ad, userid), count) => {
                val sql: String =
                  """
                    |INSERT INTO user_ad_count(dt,userid,adid,count)
                    |VALUES (?,?,?,?)
                    |ON DUPLICATE KEY
                    |UPDATE count=count+?
                    |""".stripMargin
                JdbcUtil.executeUpdate(conn, sql, Array(dt, userid, ad, count, count))

                // 检测用户单次点击是否超过30
                val sql2: String =
                  """
                    |select userid
                    |from user_ad_count
                    |where dt = ? and userid = ? and adid = ? and count > 30
                    |""".stripMargin
                val flg: Boolean = JdbcUtil.isExist(conn, sql2, Array(dt, userid, ad))
                if (flg) {
                  val sql3: String =
                    """
                      |INSERT INTO black_list(userid)
                      |VALUES (?)
                      |ON DUPLICATE KEY
                      |UPDATE userid=?
                      |""".stripMargin

                  JdbcUtil.executeUpdate(conn, sql3, Array(userid, userid))
                }
              }
            }

            conn.close()
          }
        )
      }
    )

    //    filterAdsLogDS

  }
}
