package com.stan.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object WordCountDemo {

  def main(args: Array[String]): Unit = {
    // 环境准备
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCountDemo")
    val sc = new SparkContext(conf)

    // 加载数据
    val fileRDD = sc.textFile("input/words.txt")
    // 数据处理
    val word2Count = fileRDD.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    // 打印数据
    word2Count.foreach(println)

    // 关闭资源
    sc.stop();
  }
}
