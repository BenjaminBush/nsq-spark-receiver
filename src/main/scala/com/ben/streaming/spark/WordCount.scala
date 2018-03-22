package com.ben.streaming.spark

// Spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

//Java
import java.nio.charset.StandardCharsets.UTF_8
import java.io.{FileWriter, File}

// This project
import model._

object WordCount {

  private def writeToFile(str: String, filePath: String) {
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

  def awaitTermination(ssc: StreamingContext) {
    ssc.awaitTermination()
  }

  def awaitTerminationOrTimeout(ssc: StreamingContext, timeout: Long) {
    ssc.awaitTerminationOrTimeout(timeout)
  }
}
