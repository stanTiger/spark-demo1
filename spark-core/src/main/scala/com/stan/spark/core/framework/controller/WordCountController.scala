package com.stan.spark.core.framework.controller

import com.stan.spark.core.framework.application.WordCountApp.sc
import com.stan.spark.core.framework.service.WordCountService
import org.apache.spark.rdd.RDD

class WordCountController {
  private val wordCountService = new WordCountService

  def dispatch() = {
    val word2Count: RDD[(String, Int)] = wordCountService.analysisData()
    // 打印数据
    word2Count.foreach(println)
  }
}
