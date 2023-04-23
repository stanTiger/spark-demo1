package com.stan.spark.streaming.operator.normal

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformOperator {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TransformOperator")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    // load data
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // transform 方法可以将底层RDD获取到后进行操作
    // 1. DStream 功能不完善
    // 2. 需要代码周期性的执行

    // code :  Driver 端
    lines.transform(
      rdd => {
        // code :  Driver 端 (周期性执行)
        rdd.map(
          data => {
            // code : Executor 端
            data
          }
        )
      }
    )

    // code : Driver 端
    lines.map(
      data => {
        // code : Executor 端
        data
      }
    )


    ssc.start()
    ssc.awaitTermination()
  }

}
