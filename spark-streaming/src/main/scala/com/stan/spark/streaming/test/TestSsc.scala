package com.stan.spark.streaming.test

import com.stan.spark.streaming.framework.util.EnvUtil
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TestSsc {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    EnvUtil.set(ssc)

    new TestController().dispatch()



    ssc.start()
    ssc.awaitTermination()
  }

}
