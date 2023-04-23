package com.stan.spark.core.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PageJumpRate {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("PageJumpRate")
    val sc: SparkContext = new SparkContext(conf)
    // load data
    val fileRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")
    // analysis data
    // 单页PV   (pageId,PV)
    val pagePvRDD: RDD[(String, Int)] = fileRDD.map(
      line => {
        val fields: Array[String] = line.split("_")
        (fields(3), 1)
      }
    )
      .reduceByKey(_ + _)

//    pagePvRDD.collect().foreach(println)
    // 跳转次数
    // 同一个session ,pageId 的顺序  (session,(pageId,actionTime))
    val pageJumpCount: RDD[((String, String), Int)] = fileRDD.map(
      line => {
        val fields: Array[String] = line.split("_")
        (fields(2), (fields(3), fields(4)))
      }
    )
      .groupByKey()
      .mapValues(
        // (3,5,7,8) =>  (3,5,7) zip (5,7,8) => ((3,5),(5,7),(7,8))
        iter => {
          val orderedPageId: List[String] = iter.toList.sortBy(_._2).map(_._1)
          val tail: List[String] = orderedPageId.tail
          orderedPageId.zip(tail)
        }
      )
      .flatMap(_._2)
      .map((_, 1))
      .reduceByKey(_ + _)

    pageJumpCount.collect().foreach(println)

    val joinRDD: RDD[(String, ((String, Int), Int))] = pageJumpCount.map {
      case ((prev, next), cnt) => (prev, (next, cnt))
    }
      .join(pagePvRDD)
    val jumpPageRateRDD: RDD[(String, Double)] = joinRDD.map {
      case (prev, ((next, jumpCnt), pv)) => {
        val k: String = prev + "-" + next
        val jumpRate: Double = jumpCnt.doubleValue() / pv.doubleValue()
        (k, jumpRate)
      }
    }

    jumpPageRateRDD.collect().foreach(println)

    sc.stop()
  }

}
