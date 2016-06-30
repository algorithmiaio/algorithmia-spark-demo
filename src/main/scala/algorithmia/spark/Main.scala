package algorithmia.spark

import com.algorithmia._
import com.algorithmia.algo.{FutureAlgoResponse, AlgoSuccess, AlgoResponse, AlgoFailure}
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
    // Set spark hostname here, for local host the number in parentheses is the thread count
    val SPARK_HOSTNAME = "local[100]"

    // Set # partitions for processing the input file
    val NUM_PARTITIONS = 1
    val INPUT_FILE_NAME = "file:///workspace/spark-test/test-data"

    // Algorithmia variables to set, your api key and the algorithm you want to call
    val ALGORITHMIA_API_KEY =  "/* YOUR API KEY */"
    val ALGORITHMIA_ALGO_NAME = "algo://demo/Hello" // Set the name of the algorithm you want to call here

    // In order to ensure there are enough HTTP connections to the API server set NUM_CONNECTIONS appropriately
    // for the amount of parallelism the spark task is doing.  This is primarily for local testing as in a cluster
    // mode you can just increase the number of partitions and they will run on separate hosts
    val NUM_CONNECTIONS = 20

    val conf = new SparkConf().setAppName("Algorithmia Spark Test").setMaster(SPARK_HOSTNAME)
    val spark = new SparkContext(conf)

    val textfile = spark.textFile(INPUT_FILE_NAME, NUM_PARTITIONS)

    // Use mapPartitions to avoid client creation on every record
    val results = textfile.mapPartitions(lines => {
      val client = Algorithmia.client(ALGORITHMIA_API_KEY, NUM_CONNECTIONS)

      val algo = client.algo(s"${ALGORITHMIA_ALGO_NAME}")
      // Here you could also call pipeJsonAsync which will allow each spark task to make
      // NUM_CONNECTIONS calls concurrently
      lines.map(line => {
        Try(algo.pipeJson(line))
      })
    })

    // To prevent exceptions from happening at other points it's best to filter the failed response out
    .flatMap(response => {
      response match {
        case success: Success[AlgoResponse] => {
          success.get match {
            case s: AlgoSuccess => {
              // Success! Emit algorithm output and count of 1
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
          println(s"Failure! - ${failure} - ${failure.exception} - ${failure.exception.getMessage}")
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
