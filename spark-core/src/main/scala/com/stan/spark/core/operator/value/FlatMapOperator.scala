package com.stan.spark.core.operator.value

import org.apache.spark.{SparkConf, SparkContext}

object FlatMapOperator {

  def main(args: Array[String]): Unit = {
    // 环境准备
    val conf = new SparkConf().setMaster("local[*]").setAppName("MakeRDD")
    val sc = new SparkContext(conf)

    // 算子
    val rdd = sc.makeRDD(List(List(1, 2), List(3, 5), 4))

    // 采用模式匹配分别处理不同数据类型
    val flatRDD = rdd.flatMap {
      case list: List[_] => list
      case data => List(data)
    }

    flatRDD.foreach(println)

    // 关闭连接
    sc.stop()
  }

}
