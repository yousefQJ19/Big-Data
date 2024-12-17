import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructType}

object KafkaStreamToKafkaConsumer {
  def main(args: Array[String]): Unit = {

    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("SparkKafkaConsumerExample")
      .master("local[2]") // Adjust for your cluster
      .getOrCreate()

    // Set logging level to WARN to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

    // Subscribe to Kafka topic (e.g., 'testing') and read messages
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")  // Kafka server address
      .option("subscribe", "testing")  // Kafka topic name
      .load()

    // Deserialize the Kafka message (value column) from bytes to string
    val messageStream = kafkaStream.selectExpr("CAST(value AS STRING) AS json_value")

    // Define a schema for the JSON data (assuming structure with fields text, created_at, entities (with hashtags), and geo)
    val schema = new StructType()
      .add("text", StringType)
      .add("created_at", StringType)
      .add("entities", new StructType().add("hashtags", new org.apache.spark.sql.types.ArrayType(new org.apache.spark.sql.types.StructType().add("text", StringType), true)))
      .add("geo", new StructType().add("coordinates", new org.apache.spark.sql.types.ArrayType(StringType, true)))

    // Parse the JSON string into a DataFrame with the defined schema
    val parsedStream = messageStream
      .select(from_json(col("json_value"), schema).alias("parsed"))
      .select("parsed.text", "parsed.created_at", "parsed.entities.hashtags", "parsed.geo.coordinates")

    // Extract the hashtags (if they exist) as a comma-separated string
    val hashtagsStream = parsedStream.withColumn("hashtags",
      when(col("hashtags").isNotNull, concat_ws(", ", col("hashtags.text")))
        .otherwise(lit("")))

    // Extract geolocation if available (assuming geo is present in the JSON)
    val geoStream = hashtagsStream.withColumn("geo", col("coordinates"))

    // Prepare the data to be sent to Kafka
    val kafkaStreamToSend = geoStream
      .selectExpr("CAST(null AS STRING) AS key", "CAST(geo AS STRING) AS value")  // `key` is null, `value` is geo data as a string





    // Write the stream to Kafka topic
    val query = kafkaStreamToSend
      .writeStream
      .outputMode("append")  // Append mode to keep adding new rows
      .format("kafka")  // Write to Kafka
      .option("kafka.bootstrap.servers", "localhost:9092")  // Kafka server address
      .option("topic", "ready_data")  // Kafka topic to write to
      .option("checkpointLocation", "./src/checkpoint")  // Checkpoint location for stream processing state
      .trigger(Trigger.ProcessingTime("20 seconds"))  // Process every 20 seconds
      .start()

    // Await termination of the stream
    query.awaitTermination()
  }
}
