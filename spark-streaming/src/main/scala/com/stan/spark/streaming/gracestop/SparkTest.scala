package com.stan.spark.streaming.gracestop

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkTest {

  def getSSC(): StreamingContext = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./cp")

    // load data
    val inputStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val word2One: DStream[(String, Int)] = inputStream.flatMap(_.split(" ")).map((_, 1))

    val word2Count: DStream[(String, Int)] = word2One.updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        val total: Int = buff.getOrElse(0) + seq.sum
        Option(total)
      }
    )

    word2Count.print()

    ssc
  }

  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./cp", () => getSSC())


    ssc.start()
    // 开启线程检测是否关闭
    new Thread(new MonitorStop(ssc)).start()

    ssc.awaitTermination()
  }

}
