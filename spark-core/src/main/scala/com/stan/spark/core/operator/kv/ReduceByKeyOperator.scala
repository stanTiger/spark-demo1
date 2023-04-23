package com.stan.spark.core.operator.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKeyOperator {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KVOperatorTest")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 处理数据
    val fileRDD: RDD[String] = sc.textFile("input/words.txt")
    val word2Count: RDD[(String, Int)] = fileRDD.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _,2)

    word2Count.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }

}
