package com.stan.spark.streaming.framework.util

import org.apache.spark.streaming.StreamingContext

object EnvUtil {
  private val scLocal = new ThreadLocal[StreamingContext]

  def set(sc: StreamingContext): Unit = {
    scLocal.set(sc)
  }

  def get(): StreamingContext = {
    scLocal.get()
  }

  def clear(): Unit = {
    scLocal.remove()
  }

}
