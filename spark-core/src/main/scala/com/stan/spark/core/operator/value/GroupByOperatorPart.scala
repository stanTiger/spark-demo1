package com.stan.spark.core.operator.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object GroupByOperatorPart {

  def main(args: Array[String]): Unit = {
    // 环境配置
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ValueOperatorTest")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 加载数据
    val fileRDD: RDD[String] = sc.textFile("input/apache.log")
    val timeRDD: RDD[String] = fileRDD.map(
      line => {
        val time: String = line.split(" ")(3)
        // 17/05/2015:10:05:03
        val sdf: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date: Date = sdf.parse(time)
        val hours: Int = date.getHours
        hours.toString
      }
    )
    val hourAndCount: RDD[(String, Int)] = timeRDD.groupBy(hour => hour)
      .mapValues(_.count(ele => true))

    hourAndCount.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }

}
