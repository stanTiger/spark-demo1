package com.stan.spark.core.operator.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SubstractOperator {

  def main(args: Array[String]): Unit = {
    // 环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DoubleValueOperatorTest")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 数据处理
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(5, 6, 3, 4))

    val substractRDD: RDD[Int] = rdd1.subtract(rdd2)

    substractRDD.collect().foreach(println)


    // 关闭连接
    sc.stop()

  }

}
