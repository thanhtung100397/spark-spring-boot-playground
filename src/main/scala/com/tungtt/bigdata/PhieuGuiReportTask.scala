package com.tungtt.bigdata

import com.tungtt.bigdata.entities.PhieuGui
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object PhieuGuiReportTask {

  val phieuGuiStruct: StructType = new StructType()
    .add("payload", new StructType()
      .add("after", new StructType()
        .add("id_phieugui", IntegerType, nullable = true)
        .add("ma_phieugui", StringType, nullable = true)
        .add("gui_trongnuoc", IntegerType, nullable = true)
        .add("ma_khgui", StringType, nullable = true)
        .add("ten_khgui", StringType, nullable = true)
        .add("diachi_khgui", StringType, nullable = true)
        .add("tel_khgui", StringType, nullable = true),
        nullable = true),
      nullable = true)

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

    val phieuGuiDs = ds.select(from_json($"value", phieuGuiStruct).alias("data"))
                       .map(value => {
                           value.getAs[GenericRowWithSchema]("data")
                                .getAs[GenericRowWithSchema]("payload")
                                .getAs[GenericRowWithSchema]("after")
                       })

    val streamingQuery = phieuGuiDs.writeStream
                           .outputMode("append")
                           .format("console")
                           .start()

    streamingQuery.awaitTermination()
  }
}