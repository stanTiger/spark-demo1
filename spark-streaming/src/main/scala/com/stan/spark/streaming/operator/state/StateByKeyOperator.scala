package com.stan.spark.streaming.operator.state

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StateByKeyOperator {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StateByKeyOperator")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    // 设置 checkpoint 保存点
    ssc.checkpoint("cp")
    // load data
    val inputStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val word2One: DStream[(String, Int)] = inputStream.map((_, 1))

    // 第一个参数 当前DStream 相同Key的value值
    // 第二个参数 缓存中相同Key的value值
    val stateStream: DStream[(String, Int)] = word2One.updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        val total: Int = buff.getOrElse(0) + seq.sum
        Option(total)
      }
    )
    stateStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
