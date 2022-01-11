package com.tungtt.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object PhieuGuiReportTask {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                            .master("local[1]")
                            .appName("KafkaDemo")
                            .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = spark
             .readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", "localhost:9093")
             .option("subscribe", "phieu_gui")
             .load()

    val ds = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
               .as[(String, String)]

    val streamingQuery = ds.writeStream
      .outputMode("append")
      .format("console")
      .start();

//    val phieuGuiStruct = new StructType()
//      .add("schema", new StructType().add(), nullable = true)

    streamingQuery.awaitTermination()
    ds.select("value")
  }
}