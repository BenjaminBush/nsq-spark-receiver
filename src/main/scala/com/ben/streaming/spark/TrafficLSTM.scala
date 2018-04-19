package com.ben.streaming.spark

// DL4J and ND4J
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport
import org.nd4j.linalg.factory.Nd4j
import org.nd4s.Implicits._

// Spark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

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
  def execute(master: Option[String], config: SimpleAppConfig, jars: Seq[String] = Nil): StreamingContext = {

    val sparkConf = new SparkConf().setAppName(config.appName).setJars(jars)
    for (m <- master) {
      sparkConf.setMaster(m)
    }
    val ssc = new StreamingContext(sparkConf, Seconds(config.batchDuration))

    val h5path = "/home/ben/Notebooks/lstm_model.h5"
    val lstm = KerasModelImport.importKerasSequentialModelAndWeights(h5path)

    val input = ssc.receiverStream(new NsqReceiver(config.nsq))

    val flow = input.reduce((a, b) => a)
    val flow_str = flow.toString

    val arr = (16 to 16).asNDArray(1,1)
    writeToFile(lstm.rnnTimeStep(arr).toString, config.outputFile)


    val words = input.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    wordCounts.print()
    wordCounts.foreachRDD {
      wordCountRDD => {
        var wordCountTotal = ""
        wordCountRDD.foreach {
          //wordCount => writeToFile(wordCount.toString + "\n", config.outputFile)
          wordCount => println(wordCount.toString)
            val arr = (16 to 16).asNDArray(1,1)
            writeToFile(lstm.rnnTimeStep(arr).toString, config.outputFile)
        }
      }
    }
    //    val string_flow = input.toString()
//    val int_flow = string_flow.toInt
//
//    val arr = Nd4j.create(int_flow)
//
//    println(arr)
//
//    val y_pred = lstm.predict(arr)
//
//    for (pred <- y_pred) {
//      writeToFile(pred.toString, config.outputFile)
//      println(pred)
//    }

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
