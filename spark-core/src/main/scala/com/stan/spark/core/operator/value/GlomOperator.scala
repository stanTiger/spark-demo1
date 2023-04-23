package com.stan.spark.core.operator.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GlomOperator {

  def main(args: Array[String]): Unit = {
    // 环境准备
    val conf = new SparkConf().setMaster("local[*]").setAppName("MakeRDD")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    val glomRDD: RDD[Array[Int]] = rdd.glom()

    val maxSum: Int = glomRDD.map(arr => arr.max)
      .collect()
      .sum

    println(maxSum)



    // 关闭连接
    sc.stop()
  }

}
