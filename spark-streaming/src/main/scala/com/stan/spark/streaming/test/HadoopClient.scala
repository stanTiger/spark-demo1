package com.stan.spark.streaming.test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI

object HadoopClient {

  def main(args: Array[String]): Unit = {
    val fs: FileSystem = FileSystem.get(new URI("hdfs://192.168.1.91:8020"), new Configuration(), "stan")
    //    fs.mkdirs(new Path("/xiyou/sanguo/"))
    val flg: Boolean = fs.exists(new Path("/stopSpark"))
    println(flg)
    fs.close()
  }

}
