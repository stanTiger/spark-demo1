package com.stan.spark.core.operator.value

import org.apache.spark.{SparkConf, SparkContext}

object ValueOperator {

  def main(args: Array[String]): Unit = {
    // 环境准备
    val conf = new SparkConf().setMaster("local[*]").setAppName("MakeRDD")
    val sc = new SparkContext(conf)

    // 算子
    val fileRDD = sc.textFile("input/words.txt")

    val rdd = sc.makeRDD(List(List(1, 2), List(3, 5)), 2)

    val flatRDD = rdd.flatMap(list => list)


    // 关闭连接
    sc.stop()
  }

}
