# algorithmia-spark-demo
Repo showing test code for integration with Spark.

This demo will feed the contents of a file (line-by-line) into a given Algorithmia algorithm,
group and count the output of the algorithm using Apache Spark.

How to run:
 1. Have spark installed either locally or on a cluster (see: http://spark.apache.org/)
 2. Edit src/main/scala/algorithmia/spark/Main.scala to set:
    a. ALGORITHMIA_API_KEY : Your Algorithmia API key
    b. ALGORITHMIA_ALGO_NAME : Algorithm name you'd like to call
    c. SPARK_HOSTNAME : Connection string for your spark instance
    d. INPUT_FILE_NAME : Name of the file to use as input
    e. NUM_PARTITIONS (optionally) : Number of partitions of the file spark should run
 3. Perform an "sbt run" from the top level of the project
