package com.stan.spark.streaming.wc

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    // 环境准备
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    // load data
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    // analysis data
    val word2CountDStream: DStream[(String, Int)] = socketDStream.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    word2CountDStream.print()

    // 开启
    ssc.start()
    ssc.awaitTermination()
  }

}
