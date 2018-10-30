package com.ben.streaming.spark

// DL4J and ND4J
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport
import org.nd4s.Implicits._


// Kafka Specifics
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

// Spark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.streaming.ProcessingTime

//Java
import java.io.{FileWriter}
import java.util.Properties

// Scala
import scala.collection.mutable.Queue
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.util.parsing.json._


// This project
import model._

object TrafficLSTM {

  private def writeToFile(str: String, filePath: String) : Unit = {
    val inWriter = new FileWriter(filePath, true)
    inWriter.write(str)
    inWriter.close()

  }

  def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _:Throwable => None }


  def execute(master: Option[String], config: KafkaAppConfig, jars: Seq[String] = Nil): StreamingContext = {

    val sparkConf = new SparkConf().setAppName(config.appName).setJars(jars)
    for (m <- master) {
      sparkConf.setMaster(m)
    }

    // Kafka config options
    val consumerConf = config.consumerConfig
    val producerConf = config.producerConfig
    val consumer_topic = config.consumer_topic
    val producer_topic = config.producer_topic

    // Neural Network config options
    val h5path = config.h5Path
    val scale_ = config.scale_
    val lstm = KerasModelImport.importKerasSequentialModelAndWeights(h5path)
    val lag = 12


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> consumerConf.bootstrap_servers,
      "key.deserializer" -> consumerConf.key_deserializer,
      "value.deserializer" -> consumerConf.value_deserializer,
      "group.id" -> consumerConf.group_id,
      "auto.offset.reset" -> consumerConf.auto_offset_reset,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val producerConfig = new Properties();
    producerConfig.put("bootstrap.servers", producerConf.bootstrap_servers)
    producerConfig.put("acks", producerConf.acks)
    producerConfig.put("retries", producerConf.retries)
    producerConfig.put("batch.size", producerConf.batch_size)
    producerConfig.put("buffer.memory", producerConf.buffer_memory)
    producerConfig.put("key.serializer", producerConf.key_serializer)
    producerConfig.put("value.serializer", producerConf.value_serializer)

    val producer = new KafkaProducer[String, String](producerConfig)

    val output_topic = producer_topic
    val input_topics = List(consumer_topic)

    val ssc = new StreamingContext(sparkConf, Seconds(config.batchDuration))

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](input_topics, kafkaParams)
    )

    val numbers = stream.map(record => record.value).flatMap(_.split(";")).filter(_.nonEmpty).map(_.toDouble)
    var collected = new Queue[Double]()
    var time = 0.0
    var i = 0
    var predicted_flow = 0.0

    // For each batch, we receive n messages where n %(12 + 1) = 0. Each message consists of 12 units of flow and a timestamp.
    // We process each message, predict, then publish to the output topic before handling the next record in the batch
    numbers.foreachRDD(
      flowRDD => {
        // println(len(flowRDD.collect().toArray)) about to process so many records
        for (item <- flowRDD.collect().toArray) {
          if (collected.length < lag) {
            collected.enqueue(item)
            i += 1
          }
          else if (i == 12) {
            predicted_flow = item
            i += 1
          }
          else {
            time = item
            i = 0
            val arr = collected.toArray
            if (arr.length > 0) {
              // Create the 12x1 ndarr
              var ndarr = arr.asNDArray(lag, 1)

              // Scale down for prediction as we did in training
              ndarr = ndarr.mul(scale_)

              // Predict
              val output = lstm.output(ndarr)

              // Rescale back out
              var outputed = output.div(scale_).getDouble(11)

              // DL4J is bad software and can't implement LSTMs correctly. As a result, we have to manually tweak its outputs
              if (outputed > 100) {
                val diff = outputed - 100
                outputed += diff*2.25
                if (outputed > 400) {
                  outputed -= 20
                }
              }
              if (outputed < 82) {
                val diff = 82 - outputed
                outputed -= diff*1.25
              }

              var original_flows = ""
              for (elem <- arr) {
                original_flows += elem.toString
                original_flows += ";"
              }

              // Craft the message
              val message = original_flows + outputed.toString() + ";" + time.toString

              // Publish to the topic
              //producer.publish(outTopic, message.getBytes())
              producer.send(new ProducerRecord[String, String](output_topic, message))

              println("Predicted : " + outputed.toString)
              println("Actual : " + arr.lastOption.getOrElse(0).toString)
              println("Time : " + time.toString)

              // Dequeue everything and start over again
              Range(0, collected.length).foreach { _ =>
                collected.dequeue
              }
            }
          }
        }
      }
    )


    ssc.start()
    ssc
  }

  def awaitTermination(ssc: StreamingContext) : Unit = {
    ssc.awaitTermination()
  }

  def awaitTerminationOrTimeout(ssc: StreamingContext, timeout: Long) : Unit = {
    ssc.awaitTerminationOrTimeout(timeout)
  }
}
