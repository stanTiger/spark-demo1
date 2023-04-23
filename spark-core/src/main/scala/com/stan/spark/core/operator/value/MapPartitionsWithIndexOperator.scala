package com.stan.spark.core.operator.value

import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsWithIndexOperator {

  def main(args: Array[String]): Unit = {
    // 环境准备
    val conf = new SparkConf().setMaster("local[*]").setAppName("MakeRDD")
    val sc = new SparkContext(conf)

    // 算子
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    //    val maxRDD = rdd.mapPartitionsWithIndex(
    //      (index,iter) => {
    //        if (index == 1){
    //          iter
    //        } else {
    //          Nil.iterator
    //        }
    //      }
    //    )

    val maxRDD = rdd.mapPartitionsWithIndex {
      case (1, iter) => iter
      case (_, iter) => Nil.iterator
    }

    maxRDD.foreach(println)

    // 关闭连接
    sc.stop()
  }

}
