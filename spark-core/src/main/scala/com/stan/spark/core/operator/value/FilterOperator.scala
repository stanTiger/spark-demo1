package com.stan.spark.core.operator.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object FilterOperator {

  def main(args: Array[String]): Unit = {
    // 环境配置
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ValueOperatorTest")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 加载数据
    val fileRDD: RDD[String] = sc.textFile("input/apache.log")
    val filterRDD: RDD[String] = fileRDD.filter(
      line => {
        val time: String = line.split(" ")(3)
        val sdf: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date: Date = sdf.parse(time)
        val sdf1: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val targetDate: String = sdf1.format(date)
        "2015-05-17".equals(targetDate)
      }
    )
    filterRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }

}
