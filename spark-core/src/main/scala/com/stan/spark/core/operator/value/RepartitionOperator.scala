package com.stan.spark.core.operator.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RepartitionOperator {

  def main(args: Array[String]): Unit = {
    // 环境配置
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ValueOperatorTest")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 加载数据
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 6, 8), 2)

      // 底层调用 coalesce , shuffle 参数为 true
    val repartitionRDD: RDD[Int] = rdd.repartition(4)

    repartitionRDD.saveAsTextFile("output/")


    // 关闭连接
    sc.stop()
  }

}
