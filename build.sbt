lazy val root = project.in(file("."))
  .settings(
    name        := "nsq-spark-receiver",
    version     := "1.0.3",
    description := "Project for NSQ-Spark integration"
  )
  .settings(BuildSettings.buildSettings)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.Libraries.sparkSql,
      Dependencies.Libraries.sparkStreaming,
      Dependencies.Libraries.nsqClient,
      Dependencies.Libraries.scopt,
      Dependencies.Libraries.config,
      "org.deeplearning4j" % "scalnet_2.11" % "0.9.2-SNAPSHOT",
      "org.deeplearning4j" % "deeplearning4j-core" % "0.9.2-SNAPSHOT",
      "org.nd4j" %"nd4j-native" % "0.9.2-SNAPSHOT" % "test"
    )
  )

resolvers in ThisBuild ++= Seq(Opts.resolver.sonatypeSnapshots)
assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "overview.html" => MergeStrategy.last  // Added this for 2.1.0 I think
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}