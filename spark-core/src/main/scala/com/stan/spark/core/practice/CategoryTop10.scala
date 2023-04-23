package com.stan.spark.core.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CategoryTop10 {

  def main(args: Array[String]): Unit = {
    // 环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CategoryTop10")
    val sc: SparkContext = new SparkContext(conf)
    // 数据处理
    val fileRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")
    val categoryHotRDD: RDD[(String, (Int, Int, Int))] = fileRDD.flatMap(
      data => {
        val fields: Array[String] = data.split("_")
        val clickCategoryId: String = fields(6)
        val orderCategoryIds: String = fields(8)
        val payCategoryIds: String = fields(10)
        if (clickCategoryId != "-1") {
          List((clickCategoryId, (1, 0, 0)))
        } else if (orderCategoryIds != "null") {
          val ids: Array[String] = orderCategoryIds.split(",")
          ids.map((_, (0, 1, 0)))
        } else if (payCategoryIds != "null") {
          val ids: Array[String] = payCategoryIds.split(",")
          ids.map((_, (0, 0, 1)))
        } else {
          Nil
        }
      }
    ).reduceByKey {
      case ((click1, order1, pay1), (click2, order2, pay2)) => (click1 + click2, order1 + order2, pay1 + pay2)
    }

    val result: Array[(String, (Int, Int, Int))] = categoryHotRDD.sortBy(_._2, false).take(10)

    result.foreach(println)






    // 关闭连接
    sc.stop()
  }

}
