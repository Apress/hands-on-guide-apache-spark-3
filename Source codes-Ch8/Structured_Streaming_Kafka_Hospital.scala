package org.apress.handsOnSpark3.com

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, StructField}

object SparkKafka {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .master("local[3]")
      .appName("SparkStructuredStreamingHospital")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "patient")
        .option("startingOffsets", "earliest") // From starting
        .load()

    df.printSchema()

    val PatientsSchema = StructType(Array(
      StructField("NSS", StringType),
      StructField("Nom", StringType),
      StructField("DID", IntegerType),
      StructField("DNom", StringType),
      StructField("Fecha", StringType))
    )

    val patient = df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), PatientsSchema).as("data"))
      .select("data.*")
    
    patient.printSchema()
    
    //df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

     val query = patient.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()


  }
}
