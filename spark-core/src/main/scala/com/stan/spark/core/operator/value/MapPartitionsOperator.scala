package com.stan.spark.core.operator.value

import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsOperator {

  def main(args: Array[String]): Unit = {
    // 环境准备
    val conf = new SparkConf().setMaster("local[*]").setAppName("MakeRDD")
    val sc = new SparkContext(conf)

    // 算子
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
    val maxRDD = rdd.mapPartitions(
      iter => {
        List(iter.max).toIterator
      }
    )

    maxRDD.foreach(println)

    // 关闭连接
    sc.stop()
  }

}
