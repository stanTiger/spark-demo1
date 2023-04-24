package com.stan.spark.streaming.framework.controller

import com.stan.spark.streaming.framework.service.BlackListService

class BlackListController {
  private val service = new BlackListService

  def dispatch()  = {
    service.analysisData()

  }
}

