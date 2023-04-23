package com.stan.spark.streaming.gracestop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.{StreamingContext, StreamingContextState}

import java.net.URI

class MonitorStop(ssc: StreamingContext) extends Runnable {
  override def run(): Unit = {
    // 获取 hadoop fs对象
    val fs: FileSystem = FileSystem.get(new URI("hdfs://192.168.1.91:8020"), new Configuration(), "root")

    while (true) {
      try {
        Thread.sleep(5000)
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
      val state: StreamingContextState = ssc.getState()
      // 判断指定文件是否存在
      val flg: Boolean = fs.exists(new Path("/stopSpark"))

      if (flg) {
        // 判断 streaming 是否为活跃
        if (StreamingContextState.ACTIVE == state) {
          ssc.stop(true, true)
          fs.close()
          System.exit(0)
        }
      }
    }
  }
}
