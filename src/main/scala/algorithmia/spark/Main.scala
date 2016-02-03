package algorithmia.spark

import com.algorithmia._
import com.algorithmia.algo.{AlgoSuccess, AlgoResponse, AlgoFailure}
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Try
import scala.util.Success
import scala.util.Failure

object Main extends App {

  /**
    * Calls HelloWorld algorithm for each line in a given textfile, then performs a wordCount based on the outputs
    * from the algorithms
    */
  override def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("Algorithmia Spark Test").setMaster("local[1]") // Need spark hostname here
    val spark = new SparkContext(conf)

    val textfile = spark.textFile("file:///home/patrick/workspace/testdata") // Can add # partitions if desired

    // Use mapPartitions to avoid client creation on every record
    val results = textfile.mapPartitions(lines => {
      val client = Algorithmia.client("/* YOUR API KEY */")
      val algo = client.algo("algo://pmcq/Hello/0.1.3")

      // Here we could also do batching call like algo.pipeMany
      lines.map(line => {
        Try(algo.pipeJson(line))
      })

      // Potentially would want to retry calls which threw an APIException if desired
    })

    // To prevent exceptions from happening at other points it's best to filter the failed response out
    .flatMap(response => {
      response match {
        case success: Success[AlgoResponse] => {
          success.get match {
            case s: AlgoSuccess => {
              // Success! Emit algoritm output and count of 1
              List((s.asString, 1))
            }
            case f: AlgoFailure => {
              // Algorithm encountered an error on a legitimate request
              println("Algo Failure! - " + f.error.getMessage)
              None
            }
          }
        }
        case failure: Failure[AlgoResponse] => {
          // API Exception/Bad request errors
          println("Failure! - " + failure.exception.getMessage)
          None
        }
      }
    })

    // Do some operations on your results - e.g. group and count
    .reduceByKey((a,b) => a + b)

    // Gather results back at driver for display
    .collect()

    results.map(result => println(s"${result._1} : ${result._2}"))

    spark.stop()

  }

}
