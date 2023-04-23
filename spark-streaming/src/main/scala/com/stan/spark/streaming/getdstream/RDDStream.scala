package com.stan.spark.streaming.getdstream

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RDDStream {

  def main(args: Array[String]): Unit = {
    // 环境准备
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(4))
    // 创建RDD队列
    val rddQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()
    // 创建 queueInputDStream
    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue, false)
    // 处理数据
    val reduceStream: DStream[(Int, Int)] = inputStream.map((_, 1)).reduceByKey(_ + _)

    reduceStream.print()

    // 开启
    ssc.start()
    for (elem <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }
    ssc.awaitTermination()
  }

}
