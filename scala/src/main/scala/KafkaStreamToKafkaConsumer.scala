import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructType}

object KafkaStreamToKafkaConsumer {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkKafkaConsumerExample")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "testing")
      .load()

    val messageStream = kafkaStream.selectExpr("CAST(value AS STRING) AS json_value")

    val schema = new StructType()
      .add("text", StringType)
      .add("created_at", StringType)
      .add("entities", new StructType().add("hashtags", new org.apache.spark.sql.types.ArrayType(new org.apache.spark.sql.types.StructType().add("text", StringType), true)))
      .add("geo", new StructType().add("coordinates", new org.apache.spark.sql.types.ArrayType(StringType, true)))

    val parsedStream = messageStream
      .select(from_json(col("json_value"), schema).alias("parsed"))
      .select("parsed.text", "parsed.created_at", "parsed.entities.hashtags", "parsed.geo.coordinates")

    val hashtagsStream = parsedStream.withColumn("hashtags",
      when(col("hashtags").isNotNull, concat_ws(", ", col("hashtags.text")))
        .otherwise(lit("")))

    val geoStream = hashtagsStream.withColumn("geo", col("coordinates"))

    val kafkaStreamToSend = geoStream
      .selectExpr("CAST(null AS STRING) AS key", "CAST(geo AS STRING) AS value")


    val query = kafkaStreamToSend
      .writeStream
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "ready_data")
      .option("checkpointLocation", "./src/checkpoint")
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .start()

    query.awaitTermination()
  }
}
