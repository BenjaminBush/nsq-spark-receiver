package com.ben.streaming.spark

import com.typesafe.config.Config

package model {

  case class NsqConfig(
    inTopicName: String,
    channelName: String,
    host: String,
    lookupPort: Int,
    port: Int
  )

  case class SimpleAppConfig(conf: Config) {
    val nsqConf = conf.getConfig("nsq")
    val nsq = NsqConfig(nsqConf.getString("inTopicName"),
                        nsqConf.getString("channelName"),
                        nsqConf.getString("host"),
                        nsqConf.getInt("lookupPort"),
                        nsqConf.getInt("port")
                       )
    val appName = conf.getString("appName")
    val batchDuration = conf.getLong("batchDuration")
    val outputFile = conf.getString("outputFile")
  }
}
