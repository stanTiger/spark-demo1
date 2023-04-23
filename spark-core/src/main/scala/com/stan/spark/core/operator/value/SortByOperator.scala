package com.stan.spark.core.operator.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortByOperator {

  def main(args: Array[String]): Unit = {
    // 环境配置
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ValueOperatorTest")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 加载数据
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 6, 8), 2)

    val sortRDD: RDD[Int] = rdd.sortBy(num => num)

    sortRDD.saveAsTextFile("output")


    // 关闭连接
    sc.stop()
  }

}
