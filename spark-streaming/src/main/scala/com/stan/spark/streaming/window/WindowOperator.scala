package com.stan.spark.streaming.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object WindowOperator {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TransformOperator")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    // load data
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val windowDS: DStream[(String, Int)] = lines.map((_, 1))
      .window(Seconds(6), Seconds(3))
    windowDS.reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
