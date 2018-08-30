package com.ben.streaming.spark

// DL4J and ND4J
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.AccumulatorV2
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport
import org.nd4s._
import org.nd4j.linalg.factory.Nd4j
import org.nd4s.Implicits._

import scala.collection.mutable.Queue
import scala.collection.mutable.ArrayBuffer

// Spark
import org.apache.spark.SparkConf
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD

//import org.apache.spark.mllib.linalg.{Vector, Vectors}

//Java
import java.io.{FileWriter}

// Scala
import scala.collection.mutable.ListBuffer

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


    val ssc = new StreamingContext(sparkConf, Seconds(config.batchDuration))

    //val h5path = config.h5Path
    val h5path = "/home/ben/git/research/TrafficPrediction/lstm_model.h5"
    val lstm = KerasModelImport.importKerasSequentialModelAndWeights(h5path)
//    val params = config.params
    val scale_ = 0.005076142131979695
    val lag = 12

    val input = ssc.receiverStream(new NsqReceiver(config.nsq))


    input.print(lag)

    // Collect the incoming stream of numbers, parse them as doubles
    val numbers = input.flatMap(_.split(" ")).filter(_.nonEmpty).map(_.toDouble)
    var collected = new Queue[Double]()
    numbers.foreachRDD(
      flowRDD => {
        for (item <- flowRDD.collect().toArray) {
          if (collected.length < lag) {
            collected.enqueue(item)
          }
        }

        val arr = collected.toArray
        if (arr.length > 0) {
          val ndarr = arr.asNDArray(lag, 1)
          val ndarr1 = ndarr.mul(scale_)
          val output = lstm.output(ndarr1)
          var outputed = output.div(scale_).getDouble(11)
          print("original output " + outputed.toString)
          if (outputed > 100) {
            val diff = outputed-100
            outputed += diff*2.25
          }
          if (outputed < 82) {
            val diff = 82 - outputed
            outputed -= diff*1.25
          }
          println("output : " + outputed.toString)
          println("actual : " + arr.lastOption.getOrElse(0).toString)
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
