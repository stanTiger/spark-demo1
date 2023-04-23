package com.stan.spark.streaming.reciever

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.util.Random


class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  private var flg: Boolean = true

  override def onStart(): Unit = {
    while (flg) {
      Thread.sleep(500)
      val message: String = "采集的数据是 : " + new Random().nextInt(10)
      store(message)
    }
  }

  override def onStop(): Unit = {
    flg = false
  }
}
