package com.ben.streaming.spark

// DL4J and ND4J
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork

// Spark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

//Java
import java.io.{FileWriter}

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

    val h5path = "data/model.h5"
    val lstm = KerasModelImport.importKerasSequentialModelAndWeights(h5path)



    val lines = ssc.receiverStream(new NsqReceiver(config.nsq))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    wordCounts.print()
    wordCounts.foreachRDD {
      wordCountRDD => {
        var wordCountTotal = ""
        wordCountRDD.foreach {
          wordCount => writeToFile(wordCount.toString + "\n", config.outputFile)
        }
      }
    }
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
