package com.stan.spark.core.frameworknew.util

import org.apache.spark.SparkContext

// 线程存储 sparkContext 实例
object EnvUtil {
  private val threadLocal = new ThreadLocal[SparkContext]

  def set(sc: SparkContext): Unit = {
    threadLocal.set(sc)
  }

  def take(): SparkContext = {
    threadLocal.get()
  }

  def clear(): Unit = {
    threadLocal.remove()
  }

}
