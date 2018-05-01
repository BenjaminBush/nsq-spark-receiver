package com.ben.streaming.spark

// DL4J and ND4J
import org.apache.spark.sql.DataFrame
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport
import org.nd4s._
import org.nd4j.linalg.factory.Nd4j
import org.nd4s.Implicits._

// Spark
import org.apache.spark.SparkConf
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
  def execute(master: Option[String], config: SimpleAppConfig, jars: Seq[String] = Nil): StreamingContext = {

    val sparkConf = new SparkConf().setAppName(config.appName).setJars(jars)
    for (m <- master) {
      sparkConf.setMaster(m)
    }
    val ssc = new StreamingContext(sparkConf, Seconds(config.batchDuration))

    val h5path = "/home/ben/Notebooks/lstm_model.h5"
    val lstm = KerasModelImport.importKerasSequentialModelAndWeights(h5path)
    val x_bar = 66.89326132
    val sigma = 40.9970568283


    val input = ssc.receiverStream(new NsqReceiver(config.nsq))
    val flows = input.map(x => (x.split(" "))).map(numbers => Vectors.dense(numbers.map(_.toDouble))(0))
    val flows2 = flows
    val flow_preds = flows.map(flow => lstm.rnnTimeStep(Array(flow.toInt).asNDArray(1, 1)))
    val scaled_flow_preds = flow_preds.map(pred => pred*sigma + x_bar)

    val predictions = flows2.map(flow => lstm.output((Array(flow.toInt)).asNDArray(1,1)))

    flows.print()
    flow_preds.foreachRDD(
      flow_predRDD => {
        for (item <- flow_predRDD.collect()) {
          println("rnntimestep prediction " + item.toString)
//          for (el <- item) {
//            println(el)
//          }
        }
      }
    )
    scaled_flow_preds.foreachRDD(
      scaledRDD => {
        for (item <- scaledRDD.collect()) {
          println("scaled rnntimestep prediction " + item.toString)
        }
      }
    )

    predictions.foreachRDD(
      outputRDD => {
        for (item <- outputRDD.collect()) {
          println("output prediction " + item.toString)
        }
      }
    )


//    val test3 = flows.foreachRDD( flow=> {
//      println("kanye")
//      println(flow.collect())
//    })
    //input.print()
//    val test = flows.map(flow=>flow)
//    val test2 = flows.map(flow=>(println(Array(flow).asNDArray(1, 1))))
//    val flow_preds = flows.map(flow=>(lstm.predict(Array(flow).asNDArray(1, 1))))
//    flow_preds.print()
//
//    val ret = flows.foreachRDD(
//      flowRDD => {
//        val curr_flow = flowRDD.collect()
//        curr_flow.foreach(println)
//
////        val flow_val = arr(0).toInt
////        val ndarr = Array(flow_val).asNDArray(1, 1)
////        val pred = lstm.rnnTimeStep(ndarr).toString
////        val message = "Prediction: " + pred + ", flow_val : " + flow_val.toString + ", input" + input.toString + "\n"
////        writeToFile(message, config.outputFile)
//
//        // Just here for Spark to be happy
//        input.print()
//      }
//    )


    //val flows = input.map(x=>(x.split(" "))).map(numbers => Vectors.dense(numbers.map(_.toInt)))
//    val flow = input.map((x=>(x.split(" ")(0).toInt)))
////    var arr = ListBuffer(Integer)
//    var flow_int = 0
//    val flow_arr = flow.foreachRDD(
//      flowRDD => {
////        arr += flowRDD.toString.toInt
//          flow_int = flowRDD.toString.toInt
//      }
//    )
//
//    //val arr = flow(0).asNDArray(1,1)
//    val ndarr = Array(flow_int).asNDArray(1, 1)
//    val pred = lstm.rnnTimeStep(ndarr).toString
//    val message = "Prediction: " + pred + ", Actual: " + flow_int.toString
//    writeToFile(message, config.outputFile)
//
//    try {
//      //val flowCounts = flows.map(x => (x, 1)).reduceByKey(_ + _)
//
//      val flows = input.map(x=>(x.split(" ")))
//      val flowCounts = flows
//      flowCounts.foreachRDD {
//        flowRDD => {
//          flowRDD.foreach {
//            flow => writeToFile(flowRDD.toString + "\n", config.outputFile)
//            val ndarr = Array(flowRDD.toString.toInt).asNDArray(1, 1)
//            val pred = lstm.rnnTimeStep(ndarr).toString
//            val message = "Prediction: " + pred + ", Actual: " + flowRDD.toString
//            //flow => writeToFile(message + "\n", config.outputFile)
//          }
//        }
//      }
//    } catch {
//      case nfe: java.lang.NumberFormatException => println("Not a number")
//      case e :   Exception => e.printStackTrace()
//    }
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
