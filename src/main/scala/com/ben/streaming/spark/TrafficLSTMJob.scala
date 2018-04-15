package com.ben.streaming.spark

// Java
import java.io.File

// Spark
import org.apache.spark.SparkContext

// Config
import com.typesafe.config.{Config, ConfigFactory}

// This project
import model._

object TrafficLSTMJob {
  def main(args: Array[String]) : Unit = {

    case class FileConfig(config: File = new File("."))
    val parser = new scopt.OptionParser[FileConfig]("nsq-spark-receiver") {
      head("nsq-spark-receiver", "1.0.3")
      opt[File]("config").required().valueName("<filename>")
        .action((f: File, c: FileConfig) => c.copy(f))
        .validate(f =>
          if (f.exists) success
          else failure(s"Configuration file $f does not exist")
        )
    }

    val conf = parser.parse(args, FileConfig()) match {
      case Some(c) => ConfigFactory.parseFile(c.config).resolve()
      case None    => ConfigFactory.empty()
    }


    if (conf.isEmpty()) {
      System.err.println("Empty configuration file")
      System.exit(1)
    }

    val config = SimpleAppConfig(conf)

    val ssc = TrafficLSTM.execute(
      master = None,
      config = config,
      jars   = List(SparkContext.jarOfObject(this).get)
    )

    TrafficLSTM.awaitTermination(ssc)
  }
}
