package com.stan.spark.core.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AdTop3 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AdTop3")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 加载数据
    val fileRDD: RDD[String] = sc.textFile("input/agent.log")
    // 1516609143867 6 7 64 16
    // 省份、广告 、点击量
    //  ((省份、广告),点击量)  =>  (省份,(广告，点击量))
    val provinceAdsClickCount: RDD[((String, String), Int)] = fileRDD.map(
      data => {
        val datas: Array[String] = data.split(" ")
        val province: String = datas(1)
        val ad: String = datas(4)
        ((province, ad), 1)
      }
    ).reduceByKey(_ + _)

    val provinceAdTop3: RDD[(String, List[(String, Int)])] = provinceAdsClickCount.map {
      case ((province, ad), cnt) => (province, (ad, cnt))
    }
      .groupByKey()
      .mapValues(
        iter => {
          iter.toList.sortWith((t1, t2) => t1._2 > t2._2).take(3)
        }
      )

    provinceAdTop3.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }

}
