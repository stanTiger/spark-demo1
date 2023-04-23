package com.stan.spark.streaming.reciever

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestCustomerReceiver {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CustomerReceiver")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    // load data
    val inputStream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver())

    inputStream.print()

    // 开启采集器
    ssc.start()
    ssc.awaitTermination()
  }

}
