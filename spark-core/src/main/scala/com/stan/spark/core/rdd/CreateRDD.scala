package com.stan.spark.core.rdd

import org.apache.spark.{SparkConf, SparkContext}

object CreateRDD {

  def main(args: Array[String]): Unit = {
    // 环境准备
    val conf = new SparkConf().setMaster("local[*]").setAppName("MakeRDD")
    val sc = new SparkContext(conf)

    // 从集合中创建RDD
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2 = sc.parallelize(List(5, 6, 7, 8))
    // 问外部文件中创建RDD
    val fileRDD = sc.textFile("input/words.txt")
    //

    // 关闭连接
    sc.stop()
  }

}
