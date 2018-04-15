import sbt._

object Dependencies {
  object V {
    val sparkSql       = "2.2.0"
    val sparkStreaming = "2.2.0"
    val nsqClient      = "1.0.0.RC4"
    val scopt          = "3.7.0"
    val config         = "1.3.1"
  }
  object Libraries {
    val sparkSql       = "org.apache.spark"      %% "spark-sql"            % V.sparkSql
    val sparkStreaming = ("org.apache.spark"      %% "spark-streaming"      % V.sparkStreaming)
    val nsqClient      = "com.github.brainlag" %  "nsq-client" % V.nsqClient
    val scopt          = "com.github.scopt"      %% "scopt"                % V.scopt
    val config         = "com.typesafe"          %  "config"               % V.config
  }
}
