
// spark-submit --class org.apress.handsOnSpark3.textFileStream --master "local[2]" /Users/aantolinez/Apress/Chapter6/textFileStream/target/scala-2.12/HandsOnSpark3-textFileStream-assembly-fatjar-1.0.jar

package org.apress.handsOnSpark3

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.io.IOException

object textFileStream {
  def main(args: Array[String]): Unit = {

    val folder="/tmp/patient_streaming"

    try {
      val spark: SparkSession = SparkSession.builder()
        .master("local[1]")
        .appName("Hand-On-Spark3_textFileStream")
        .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      val sc = spark.sparkContext
      val ssc = new StreamingContext(sc, Seconds(5))

      val lines = ssc.textFileStream(folder)

      printf(f"\n Spark is monitoring the folder $folder%s and ready... \n")

      val filterHeaders = lines.filter(!_.matches("[^0-9]+"))
      val selectedRecords = filterHeaders.map { row =>
        val rowArray = row.split(",")
        (rowArray(3))
      }
      selectedRecords.map(x => (x, 1)).reduceByKey(_ + _).print()
      //lines.print()
      ssc.start()
      ssc.awaitTermination()

    } catch {
      case e: IOException => println("IOException occurred")
      case t: Throwable => println("Error receiving data", t)
    } finally {
      println("Finally block")
    }
  }
}
