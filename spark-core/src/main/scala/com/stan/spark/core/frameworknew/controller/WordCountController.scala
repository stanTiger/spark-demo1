package com.stan.spark.core.frameworknew.controller

import com.stan.spark.core.frameworknew.common.TController
import com.stan.spark.core.frameworknew.service.WordCountService
import org.apache.spark.rdd.RDD

class WordCountController extends TController {
  private val wordCountService = new WordCountService

  def dispatch(): Unit = {

    val word2Count: RDD[(String, Int)] = wordCountService.analysisData()

    // 打印数据
    word2Count.foreach(println)
  }
}
