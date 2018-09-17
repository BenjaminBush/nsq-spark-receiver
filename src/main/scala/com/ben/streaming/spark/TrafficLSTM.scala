package com.ben.streaming.spark

// DL4J and ND4J
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport
import org.nd4s.Implicits._

// NSQ Specifics
import com.sproutsocial.nsq._

// Spark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

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

//  def parseJSONData(s: String) : (Any) = {
//    val parsed = JSON.parseFull(s)
//    val numbers = parsed.get.asInstanceOf[Map[String, String]].get("data")
//    return numbers
//  }
//
//  def parseJSONTime(s: String) : (Any) = {
//    val parsed = JSON.parseFull(s)
//    val time = parsed.get.asInstanceOf[Map[String, String]].get("time")
//    return time
//  }


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

    val h5path = config.h5Path
    val scale_ = config.scale_
    val lstm = KerasModelImport.importKerasSequentialModelAndWeights(h5path)
    val lag = 12

    val input = ssc.receiverStream(new NsqReceiver(config.nsq))

    input.print(lag)


    // Collect the incoming stream of numbers, parse them as doubles
    val numbers = input.flatMap(_.split(" ")).filter(_.nonEmpty).map(_.toDouble)
    var collected = new Queue[Double]()
    var time = 0.0
    numbers.foreachRDD(
      flowRDD => {
        for (item <- flowRDD.collect().toArray) {
          if (collected.length < lag) {
            collected.enqueue(item)
          }
          else {
            time = item
          }
        }

        val arr = collected.toArray
        if (arr.length > 0) {
          val ndarr = arr.asNDArray(lag, 1)
          val ndarr1 = ndarr.mul(scale_)
          val output = lstm.output(ndarr1)
          var outputed = output.div(scale_).getDouble(11)
          if (outputed > 100) {
            val diff = outputed-100
            outputed += diff*2.25
            if (outputed > 400) {
              outputed -= 20
            }
          }
          if (outputed < 82) {
            val diff = 82 - outputed
            outputed -= diff*1.25
          }
//          println("Predicted : " + outputed.toString)
//          println("Actual : " + arr.lastOption.getOrElse(0).toString)

          val message = outputed.toString() + ";" + time.toString

          producer.publish(outTopic, message.getBytes());

        }


        // Dequeue everything
        Range(0, collected.length).foreach { _ =>
          collected.dequeue
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
