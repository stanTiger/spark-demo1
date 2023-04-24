package com.stan.spark.core.frameworknew.service

import com.stan.spark.core.frameworknew.common.TService
import com.stan.spark.core.frameworknew.dao.WordCountDao
import org.apache.spark.rdd.RDD

class WordCountService extends TService {
  private val wordCountDao = new WordCountDao

  def analysisData(): RDD[(String, Int)] = {
    // 加载数据
    val fileRDD: RDD[String] = wordCountDao.load("input/words.txt")
    // 数据处理
    val word2Count: RDD[(String, Int)] = fileRDD.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    word2Count
  }


}
