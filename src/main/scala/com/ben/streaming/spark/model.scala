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

  case class kafkaConsumerConfig(
                                bootstrap_servers: String,
                                key_deserializer: String,
                                value_deserializer: String,
                                group_id: String,
                                auto_offset_reset: String,
                                enable_auto_commit: String
                                )
  case class kafkaProducerConfig(
                                bootstrap_servers: String,
                                acks: String,
                                retries: String,
                                batch_size: String,
                                buffer_memory: String,
                                key_serializer: String,
                                value_serializer: String
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

  case class KafkaAppConfig(conf: Config) {
    val kafkaConsumerConf = conf.getConfig("kafkaConsumer")
    val kafkaProducerConf = conf.getConfig("kafkaProducer")

    val consumerConfig = kafkaConsumerConfig(kafkaConsumerConf.getString("bootstrap.servers"),
      kafkaConsumerConf.getString("key.deserializer"),
      kafkaConsumerConf.getString("value.deserializer"),
      kafkaConsumerConf.getString("group.id"),
      kafkaConsumerConf.getString("auto.offset.reset"),
      kafkaConsumerConf.getString("enable.auto.commit"))

    val producerConfig = kafkaProducerConfig(kafkaProducerConf.getString("bootstrap.servers"),
      kafkaProducerConf.getString("acks"),
      kafkaProducerConf.getString("retries"),
      kafkaProducerConf.getString("batch.size"),
      kafkaProducerConf.getString("buffer.memory"),
      kafkaProducerConf.getString("key.serializer"),
      kafkaProducerConf.getString("value.serializer"))


    val consumer_topic = conf.getString("consumer_topic")
    val producer_topic = conf.getString("producer_topic")

    val appName = conf.getString("appName")
    val batchDuration = conf.getLong("batchDuration")
    val outputFile = conf.getString("outputFile")
    val h5Path = conf.getString("h5Path")
    val scale_ = 0.005076142131979695


  }
}
