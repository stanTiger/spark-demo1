package com.stan.spark.streaming.test

import com.stan.spark.streaming.framework.util.EnvUtil
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream

class TestController {

  def dispatch() = {
    val ssc: StreamingContext = EnvUtil.get()
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    dstream.map((_, 1)).print()
  }

}
