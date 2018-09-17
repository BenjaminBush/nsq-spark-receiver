import sbt._

object Dependencies {
  object V {
    val sparkSql       = "2.2.0"
    val sparkmllib     = "2.2.0"
    val sparkStreaming = "2.2.0"
    val nsqClient      = "1.0.0.RC4"
    val nsqProducer    = "0.9.4"
    val scopt          = "3.7.0"
    val config         = "1.3.1"
    val dl4jVer         = "1.0.0-alpha"
    val nd4jVer        = "1.0.0-alpha"
    val json4sNativeVer= "3.6.1"
  }
  object Libraries {
    val sparkSql       = "org.apache.spark"      %% "spark-sql"            % V.sparkSql
    val sparkmllib     = "org.apache.spark"       %% "spark-mllib"         % V.sparkmllib
    val sparkStreaming = ("org.apache.spark"      %% "spark-streaming"      % V.sparkStreaming)
    val nsqClient      = "com.github.brainlag" %  "nsq-client" % V.nsqClient
    val nsqProducer    = "com.sproutsocial" %     "nsq-j"   % V.nsqProducer
    val scopt          = "com.github.scopt"      %% "scopt"                % V.scopt
    val config         = "com.typesafe"          %  "config"               % V.config
    val dl4jCore       = "org.deeplearning4j" % "rl4j-core" % V.dl4jVer
    val nd4jNative     = "org.nd4j" % "nd4j-native" % V.nd4jVer
    val nd4jNativePlatform = "org.nd4j" % "nd4j-native-platform" % V.nd4jVer
    val nd4s           = "org.nd4j" %% "nd4s" % V.nd4jVer
    val json4sNative    = "org.json4s" %% "json4s-native" % V.json4sNativeVer
  }
}
