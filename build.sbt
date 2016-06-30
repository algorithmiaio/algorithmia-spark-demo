name := "spark-test"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.6.0",
  "com.algorithmia" % "algorithmia-client" % "1.0.8"
)

mainClass := Some("algorithmia.spark.Main")
