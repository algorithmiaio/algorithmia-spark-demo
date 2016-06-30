# algorithmia-spark-demo
Repo showing test code for integration with Spark.

This demo will feed the contents of a file (line-by-line) into a given Algorithmia algorithm,
group and count the output of the algorithm using Apache Spark.

# How to run:
1. Have spark installed either locally or on a cluster (see: http://spark.apache.org/)
2. Edit src/main/scala/algorithmia/spark/Main.scala to set:
    1. ALGORITHMIA_API_KEY : Your Algorithmia API key
    2. ALGORITHMIA_ALGO_NAME : Algorithm name you'd like to call
    3. SPARK_HOSTNAME : Connection string for your spark instance
    4. INPUT_FILE_NAME : Name of the file to use as input
    5. NUM_PARTITIONS (optionally) : Number of partitions of the file spark should run
3. Perform an "sbt run" from the top level of the project
