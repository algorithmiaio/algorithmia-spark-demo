package algorithmia.spark

import com.algorithmia._
import com.algorithmia.algo.{AlgoSuccess, AlgoResponse, AlgoFailure}
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Try
import scala.util.Success
import scala.util.Failure
import scala.collection.JavaConverters._
object Main extends App {

  /**
    * Gets a list of files from the data API, calls image tagger on each image,
    * extracts the most probable tag, and then does a group/count on the tags
    */
  override def main (args: Array[String]) {

    // Set spark hostname here, for local host the number in parentheses is the thread count
    val SPARK_HOSTNAME = "local[5]"

    // Set # partitions for processing the input file
    val NUM_PARTITIONS = 2
    val INPUT_DIRECTORY = "data://pmcq/Images"  // This can be any Data API path (i.e. data://, dropbox://, s3://)

    // Algorithmia variables to set, your api key and the algorithm you want to call
    val ALGORITHMIA_API_KEY =  "/* YOUR API KEY */"
    val IMAGE_TAGGING_ALGO = "algo://zeryx/inception3/0.1.1" // Set the name of the algorithm you want to call here
    val TAG_EXTRACTOR_ALGO = "algo://pmcq/ExtractMaxTag/0.1.0"

    // In order to ensure there are enough HTTP connections to the API server set NUM_CONNECTIONS appropriately
    // for the amount of parallelism the spark task is doing.  This is primarily for local testing as in a cluster
    // mode you can just increase the number of partitions and they will run on separate hosts
    val NUM_CONNECTIONS = 25
    val client = Algorithmia.client(ALGORITHMIA_API_KEY, NUM_CONNECTIONS)

    // Get list of image names by iterating the data api)
    val input_files = client.dir(INPUT_DIRECTORY).files.asScala.toList.map(file => file.toString)

    val conf = new SparkConf().setAppName("Algorithmia Spark Test").setMaster(SPARK_HOSTNAME)
    val spark = new SparkContext(conf)

    // Use mapPartitions to avoid client creation on every record
    val results = spark.parallelize(input_files, NUM_PARTITIONS)
      .mapPartitions(lines => {
        val client = Algorithmia.client(ALGORITHMIA_API_KEY, NUM_CONNECTIONS)

        val tagger_algo = client.algo(IMAGE_TAGGING_ALGO)
        // Here you could also call pipeJsonAsync which will allow each spark task to make
        // NUM_CONNECTIONS calls concurrently, though there is a higher chance you will be rate limited
        lines.map(data_file => {
          Try(tagger_algo.pipe(data_file))
        })
    })

    // An exception might get thrown from an algorithm timeout, but a successful API call where the algorithm
    // had an error results in an AlgoFailure
    // To prevent exceptions from happening at other points it's best to filter the failed response out
    .map(response => {
        val client = Algorithmia.client(ALGORITHMIA_API_KEY, NUM_CONNECTIONS)
        val extractor_algo = client.algo(TAG_EXTRACTOR_ALGO)
        response match {
          case success: Success[AlgoResponse] => {
            success.get match {
              case s: AlgoSuccess => {
                Try(extractor_algo.pipeJson(s.asJsonString()))
              }
              case f: AlgoFailure => Failure(f.error)
            }
          }
          case failure: Failure[AlgoResponse] => {
            println(s"Failure! - ${failure} - ${failure.exception} - ${failure.exception.getMessage}")
            None
          }
        }
    })

    .flatMap(response => {
      response match {
        case success: Success[AlgoResponse] => {
          success.get match {
            case s: AlgoSuccess => {
              // Success! Extract the maximum likelihood tag
              Some((s.asString(), 1))
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

    // Group and count the tags
    .reduceByKey((a,b) => a + b, NUM_PARTITIONS)

    .sortBy(key => key._2, false, NUM_PARTITIONS)

    // Gather results back at driver for display
    .collect()

    results.map(result => println(s"${result._1} : ${result._2}"))

    spark.stop()
  }

}
