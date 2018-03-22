package com.ben.streaming.spark

// Java
import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.io.File

// Spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

// Config
import com.typesafe.config.{Config, ConfigFactory}

// This project
import model._

object WordCountJob {
  def main(args: Array[String]) {

    case class FileConfig(config: File = new File("."))
    val parser = new scopt.OptionParser[FileConfig](generated.Settings.name) {
      head(generated.Settings.name, generated.Settings.version)
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

    val ssc = WordCount.execute(
      master = None,
      config = config,
      jars   = List(SparkContext.jarOfObject(this).get)
    )

    WordCount.awaitTermination(ssc)
  }
}
