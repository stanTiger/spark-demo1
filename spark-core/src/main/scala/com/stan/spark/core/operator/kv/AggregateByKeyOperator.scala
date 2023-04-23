package com.stan.spark.core.operator.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyOperator {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KVOperatorTest")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 处理数据
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("c", 3), ("b", 4), ("c", 5), ("c", 6)), 2)

    val resultRDD: RDD[(String, Int)] = rdd1.aggregateByKey(0)(Math.max, _ + _)

    resultRDD.collect().foreach(println)


    // 关闭连接
    sc.stop()
  }

}
