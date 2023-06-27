// spark-submit ./HandsOnSpark3-readStreamSocket-assembly-fatjar-1.0.jar org.apress.handsOnSpark3.readStreamSocket localhost 9999
package org.apress.handsOnSpark3

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.io.IOException

object socketTextStream {
  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 9999

    try {
      val spark: SparkSession = SparkSession.builder()
        .master("local[*]")
        .appName("Hand-On-Spark3_socketTextStream")
        .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      val sc = spark.sparkContext
      val ssc = new StreamingContext(sc, Seconds(5))

      val lines = ssc.socketTextStream(host, port)

      printf("\n Spark is listening on port 9999 and ready... \n")

      lines.print()
      ssc.start()
      ssc.awaitTermination()

    } catch {
      case e: java.net.ConnectException => println("Error establishing connection to " + host + ":" + port)
      case e: IOException => println("IOException occurred")
      case t: Throwable => println("Error receiving data", t)
    } finally {
      println("Finally block")
    }
  }
}
