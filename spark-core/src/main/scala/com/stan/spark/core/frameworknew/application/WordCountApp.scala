package com.stan.spark.core.frameworknew.application

import com.stan.spark.core.frameworknew.common.TApplication
import com.stan.spark.core.frameworknew.controller.WordCountController

object WordCountApp extends App with TApplication {

  start() {
    new WordCountController().dispatch()
  }


}
