package com.stan.spark.core.frameworknew.common

import com.stan.spark.core.frameworknew.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {

  def start(master: String = "local[*]", app: String = "APP")(op: => Unit): Unit = {
    // 环境准备
    val conf: SparkConf = new SparkConf().setMaster(master).setAppName(app)
    val sc: SparkContext = new SparkContext(conf)
    EnvUtil.set(sc)

    try {
      op
    } catch {
      case ex: Exception => println(ex.getMessage)
    }
    // 关闭资源
    sc.stop()
    EnvUtil.clear()
  }

}
