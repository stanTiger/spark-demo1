package com.stan.spark.streaming.framework.application

import com.stan.spark.streaming.framework.common.TApplication
import com.stan.spark.streaming.framework.controller.BlackListController

object RealTimeApp extends App with TApplication {

  start() {
    new BlackListController().dispatch()

  }
}
