package com.stan.spark.core.frameworknew.common

import com.stan.spark.core.frameworknew.util.EnvUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait TDao {

  def load(path: String): RDD[String] = {
    val sc: SparkContext = EnvUtil.take()
    sc.textFile(path)
  }

}
