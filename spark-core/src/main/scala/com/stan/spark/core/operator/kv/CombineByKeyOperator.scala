package com.stan.spark.core.operator.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CombineByKeyOperator {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KVOperatorTest")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 处理数据
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)

    val combineRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
      value => (value, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    val avgRDD: RDD[(String, Int)] = combineRDD.map {
      case (k, (sum, count)) => (k, sum / count)
    }

    avgRDD.collect().foreach(println)


    // 关闭连接
    sc.stop()
  }

}
