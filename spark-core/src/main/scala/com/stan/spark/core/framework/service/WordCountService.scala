package com.stan.spark.core.framework.service

import com.stan.spark.core.framework.dao.WordCountDAO
import org.apache.spark.rdd.RDD

class WordCountService {
  private val wordCountDao = new WordCountDAO

  def analysisData(): RDD[(String, Int)] = {
    // 加载数据
    val fileRDD: RDD[String] = wordCountDao.load("input/words.txt")
    // 数据处理
    val word2Count = fileRDD.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    word2Count
  }

}
