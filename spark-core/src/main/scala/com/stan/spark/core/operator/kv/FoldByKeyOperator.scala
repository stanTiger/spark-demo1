package com.stan.spark.core.operator.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FoldByKeyOperator {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KVOperatorTest")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 处理数据
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("c", 3), ("b", 4), ("c", 5), ("c", 6)), 2)

    val word2Count: RDD[(String, Int)] = rdd1.foldByKey(0)(_ + _)

    word2Count.collect().foreach(println)


    // 关闭连接
    sc.stop()
  }

}
