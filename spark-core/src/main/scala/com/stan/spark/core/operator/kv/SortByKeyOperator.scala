package com.stan.spark.core.operator.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortByKeyOperator {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KVOperatorTest")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 处理数据
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)

    val sortRDD: RDD[(String, Int)] = rdd.sortByKey()

    sortRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }

}
