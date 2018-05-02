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

class ArrayAccumulator extends AccumulatorV2[Array[Double], Array[Double]] {
  private val myArray : Array[Double] = Array.emptyDoubleArray

  def reset() : Unit = {
    for (i <- 0 until myArray.length) {
      myArray(i) = 0
    }
  }

  override def isZero: Boolean = {
    false
  }

  override def copy(): AccumulatorV2[Array[Double], Array[Double]] = {
    val c = new Array[Double](myArray.length)
    for (i <- 0 until myArray.length) {
      c(i) = myArray(i)
    }
    return this
  }

  override def add(v: Array[Double]): Unit = {
    myArray ++ v
  }

  override def merge(other: AccumulatorV2[Array[Double], Array[Double]]): Unit = {
    myArray++other.value
  }

  override def value: Array[Double] = {
    myArray
  }
}

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

    val myacc = new ArrayAccumulator

    val ssc = new StreamingContext(sparkConf, Seconds(config.batchDuration))

    ssc.sparkContext.register(myacc, "my accumulator")

    val h5path = "/home/ben/Notebooks/lstm_model.h5"
    val lstm = KerasModelImport.importKerasSequentialModelAndWeights(h5path)
    val x_bar = 66.89326132
    val sigma = 40.9970568283
    val scale = 0.00507614
    val lag = 12

    val input = ssc.receiverStream(new NsqReceiver(config.nsq))


    input.print(lag)




//    val numbers = input.map(_.split(" ")).filter(_.nonEmpty)
//
//    val flows = numbers.map(x => x.map(str=>parseDouble(str).getOrElse(-1)))
//    val filtered_flows = flows.map(x => x.filter(d => d.!=(-1) ))



    // This works:

//    val numbers = input.flatMap(_.split(" ")).filter(_.nonEmpty).map(x=>parseDouble(x))
//    numbers.print()


    // This is good code, just trying something new
    val numbers = input.flatMap(_.split(" ")).filter(_.nonEmpty).map(_.toDouble*scale)
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
          val output = lstm.output(ndarr)
          val mean = output.mean(0)
          val preds = mean.div(scale)
          println("Predicted: " + preds.toString)
        }

        //val ndarr = arr.asNDArray(lag, 1)
        //println(ndarr)

        // Dequeue everything
        Range(0, collected.length).foreach { _ =>
          collected.dequeue
        }


      }
    )




//        val output = lstm.rnnTimeStep(ndarr)

        // This part is good for testing
//        val arr1 = Array(0.04,  0.06,  0.03,  0.05,  0.05,  0.01,  0.03,  0.03,  0.03,  0.02,  0.02,  0.04)
//        val arr2 = Array(0.06,  0.03,  0.05,  0.05,  0.01,  0.03,  0.03,  0.03,  0.02,  0.02,  0.04,  0.03)
//        val arr3 = Array(0.03,  0.05,  0.05,  0.01,  0.03,  0.03,  0.03,  0.02,  0.02,  0.04,  0.03,  0.01)
//
//
//        val ndarr2 = Nd4j.create(arr2).transpose()
//        val output = lstm.rnnTimeStep(ndarr2)
//        println("Output was : " + output.toString)
//        val mean = output.mean(0)
//        println("Mean is : " + mean.toString)
//        val preds = mean.div(scale)
//        println("Predicted: " + preds.toString)


//        println(ndarr.toString)
//        println(".")
//        println(".")
//        val output = lstm.rnnTimeStep(ndarr)
//        val mean = output.mean(0)
//
//        val preds = mean.mul(scale)
//        println("Prediction: " + preds.toString)
//        val ndarr = Nd4j.create(collected.toArray)
//        val output = lstm.rnnTimeStep(ndarr)

//        val ndarr = arr.asNDArray(lag, 1)
//
//        val output = lstm.rnnTimeStep(ndarr)
//        val mean = output.mean(0)
//
//        val preds = mean.mul(scale)
//
//        println("Prediction: " + preds.toString)
//      }
//    )












//    val flows = input.map(x => (x.split(" "))).map(numbers => Vectors.dense(numbers.map(_.toDouble*scale))(0))
//    flows.print()
//    val ndarr = flows.map(flow => Array(flow).asNDArray(lag, 1))
//    val flow_preds = flows.map(flow => lstm.rnnTimeStep(Array(flow).asNDArray(lag, 1)))
//    val scaled_flow_preds = flow_preds.map(pred => pred/sigma)


//    flows.print()
//    flow_preds.foreachRDD(
//      flow_predRDD => {
//        for (item <- flow_predRDD.collect()) {
//          println("rnntimestep prediction " + item.toString)
////          for (el <- item) {
////            println(el)
////          }
//        }
//      }
//    )
//    scaled_flow_preds.foreachRDD(
//      scaledRDD => {
//        for (item <- scaledRDD.collect()) {
//          println("scaled rnntimestep prediction " + item.toString)
//        }
//      }
//    )


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
