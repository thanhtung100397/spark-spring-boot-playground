package com.tungtt.bigdata

import org.apache.spark.sql.SparkSession

object KafkaDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
                .master("local[1]")
                .appName("KafkaDemo")
                .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

//    val df = spark
//             .readStream
//             .format("kafka")
//             .option("kafka.bootstrap.servers", "172.31.24.27:9092")
//             .option("subscribe", "tungtt-demo")
//             .load()
//
//    val ds = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//      .as[(String, String)]
//
//    val stream = ds.writeStream
//      .outputMode("append")
//      .format("console")
//      .start()
//
//    // Start running the query that prints the data getting from Kafka 'test' topic
//    val streaminqQuery = df.writeStream
//                           .outputMode("append")
//                           .format("console")
//                           .start()
//    stream.awaitTermination()


  }
}
