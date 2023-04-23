package com.stan.spark.core.operator.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object SampleOperator {

  def main(args: Array[String]): Unit = {
    // 环境配置
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ValueOperatorTest")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 加载数据
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 1)

    //    val sampleRDD: RDD[Int] = rdd.sample(false, 0.5)
    //    sampleRDD.collect().foreach(println)
    val sampleRDD: RDD[Int] = rdd.sample(true, 3)
    sampleRDD.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }

}
