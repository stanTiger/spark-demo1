package com.stan.spark.streaming.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object JoinTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JoinTest")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 4)))

    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    joinRDD.collect().foreach(println)

    sc.stop()
  }

}
