import sbt._
import Keys._
import com.typesafe.config.Config

object BuildSettings {

  // Basic settings for our app
  lazy val basicSettings = Seq(
    organization          :=  "com.ben.streaming",
    scalaVersion          :=  "2.11.0",
    scalacOptions         :=  compilerOptions,
    javacOptions          :=  javaCompilerOptions,
    resolvers             +=  Resolver.jcenterRepo
  )

  lazy val compilerOptions = Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-unchecked",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Xfuture",
    "-Xlint"
  )

  lazy val javaCompilerOptions = Seq(
    "-source", "1.8",
    "-target", "1.8"
  )

  // Makes our SBT app settings available from within the app
  lazy val scalifySettings = Seq(
    sourceGenerators in Compile += Def.task {
      val file = (sourceManaged in Compile).value / "settings.scala"
      IO.write(file, """package com.ben.streaming.spark.generated
                       |object Settings {
                       |  val organization = "%s"
                       |  val version = "%s"
                       |  val name = "%s"
                       |}
                       |""".stripMargin.format(organization.value, version.value, name.value))
      Seq(file)
    }.taskValue
  )
  lazy val buildSettings = basicSettings ++ scalifySettings
}