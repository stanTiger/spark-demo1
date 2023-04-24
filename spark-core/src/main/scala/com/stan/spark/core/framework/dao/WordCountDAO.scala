package com.stan.spark.core.framework.dao

import com.stan.spark.core.framework.application.WordCountApp.sc
import org.apache.spark.rdd.RDD

class WordCountDAO {

  def load(path: String): RDD[String] = {
    sc.textFile(path)
  }

}
