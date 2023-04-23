package com.stan.spark.core.operator.value

import org.apache.spark.{SparkConf, SparkContext}

object MapOperator {

  def main(args: Array[String]): Unit = {
    // 环境准备
    val conf = new SparkConf().setMaster("local[*]").setAppName("MakeRDD")
    val sc = new SparkContext(conf)

    // 算子
    val fileRDD = sc.textFile("input/apache.log")

    val urlRDD = fileRDD.map(
      line => {
        line.split(" ")(6)
      })

    urlRDD.foreach(println)

    // 关闭连接
    sc.stop()
  }

}
