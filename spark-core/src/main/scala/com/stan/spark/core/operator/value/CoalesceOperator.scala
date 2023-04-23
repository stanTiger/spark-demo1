package com.stan.spark.core.operator.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CoalesceOperator {

  def main(args: Array[String]): Unit = {
    // 环境配置
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ValueOperatorTest")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 加载数据
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 6, 8), 6)

    val rdd2: RDD[Int] = rdd.coalesce(2)
      .mapPartitionsWithIndex {
        case (index, iter) => {
          println(index)
          iter
        }
      }

    rdd2.collect().foreach(println)


    // 关闭连接
    sc.stop()
  }

}
