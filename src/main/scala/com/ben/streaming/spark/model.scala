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

  case class ProducerConfig(outTopicName: String,
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
    val producerConf = conf.getConfig("producer")
    val producer = ProducerConfig(producerConf.getString("outTopicName"),
      producerConf.getString("channelName"),
      producerConf.getString("host"),
      producerConf.getInt("lookupPort"),
      producerConf.getInt("port")
    )
    val appName = conf.getString("appName")
    val batchDuration = conf.getLong("batchDuration")
    val outputFile = conf.getString("outputFile")
    val h5Path = conf.getString("h5Path")
    val scale_ = 0.005076142131979695                          // hard-coded for now, should subtract ~20 for values greater than 400
  }
}
