package com.stan.spark.streaming.framework.common

import com.stan.spark.streaming.framework.util.EnvUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait TApplication {

  def start(master: String = "local[*]", app: String = "RealTimeApp")(op: => Unit): Unit = {
    val conf: SparkConf = new SparkConf().setMaster(master).setAppName(app)
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    EnvUtil.set(ssc)
    try {
      op
    } catch {
      case exception: Exception => println(exception.getMessage)
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
