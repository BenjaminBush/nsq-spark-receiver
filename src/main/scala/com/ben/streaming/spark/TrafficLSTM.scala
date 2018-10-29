package com.ben.streaming.spark

// DL4J and ND4J
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport
import org.nd4s.Implicits._

import scala.collection.mutable.ArrayBuffer

// NSQ Specifics
import com.sproutsocial.nsq._

// Spark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.streaming.ProcessingTime

//Java
import java.io.{FileWriter}

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


  def execute(master: Option[String], config: SimpleAppConfig, jars: Seq[String] = Nil): StreamingContext = {

    val sparkConf = new SparkConf().setAppName(config.appName).setJars(jars)
    for (m <- master) {
      sparkConf.setMaster(m)
    }

    // Producer settings
    val producerConf = config.producer
    val host = producerConf.host
    val port = producerConf.port
    val outTopic = producerConf.outTopicName
    val lookupPort = producerConf.lookupPort
    val channelName = producerConf.channelName
    var producer = new Publisher(host)

    val ssc = new StreamingContext(sparkConf, Seconds(config.batchDuration))

    // Neural Network Constants
    val h5path = config.h5Path
    val scale_ = config.scale_
    val lstm = KerasModelImport.importKerasSequentialModelAndWeights(h5path)
    val lag = 12

    val input = ssc.receiverStream(new NsqReceiver(config.nsq))

    // Collect the incoming stream of numbers, parse them as doubles
    val numbers = input.flatMap(_.split(" ")).filter(_.nonEmpty).map(_.toDouble)
    var collected = new Queue[Double]()
    var time = 0.0

    // For each batch, we receive n messages where n %(12 + 1) = 0. Each message consists of 12 units of flow and a timestamp.
    // We process each message, predict, then publish to the output topic before handling the next record in the batch
    numbers.foreachRDD(
      flowRDD => {
        // println(len(flowRDD.collect().toArray)) about to process so many records
        for (item <- flowRDD.collect().toArray) {
          if (collected.length < lag) {
            collected.enqueue(item)
          }
          else {
            time = item
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

              // Craft the message
              val message = outputed.toString() + ";" + arr.lastOption.getOrElse(0) + ";" + time.toString

              // Publish to the topic
              producer.publish(outTopic, message.getBytes())

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
