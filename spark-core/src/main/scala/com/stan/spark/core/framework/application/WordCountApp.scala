package com.stan.spark.core.framework.application

import com.stan.spark.core.framework.controller.WordCountController
import org.apache.spark.{SparkConf, SparkContext}

object WordCountApp extends App {
  // 环境准备
  val conf = new SparkConf().setMaster("local[*]").setAppName("WordCountDemo")
  val sc = new SparkContext(conf)
  val controller: WordCountController = new WordCountController()
  controller.dispatch()

  // 关闭资源
  sc.stop();
}
