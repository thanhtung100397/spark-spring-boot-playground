package com.tungtt.bigdata

import com.tungtt.bigdata.models.SyncData
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}

import java.sql.{Date, Timestamp}

object PhieuGuiReportTask {

  case class PhieuGui(var ma_khgui: String,
                      var ngay_nhap_may: Date,
                      var tong_tien: BigDecimal) {

  }

  case class BaoCaoKhachHang(var ma_khgui: String,
                             var ngay_nhap_may: Date,
                             var san_luong: Long,
                             var doanh_thu: BigDecimal) {

  }

  val postgresqlSinkOptions: Map[String, String] = Map(
    "dbtable" -> "bao_cao_khach_hang_kafka",
    "user" -> "",
    "password" -> "",
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://<ip>:5432/postgres"
  )

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

    val syncDataStruct = Encoders.product[SyncData[PhieuGui]].schema

    val ds = df.selectExpr("CAST(value AS STRING)", "timestamp").as[(String, Timestamp)]
               .select(from_json(col("value"), syncDataStruct).as[SyncData[PhieuGui]].alias("value"), col("timestamp"))
               .select("value.payload.after.*", "timestamp")
               .withWatermark("timestamp", "1 minute")
               .groupBy(
                 window(col("timestamp"), "1 minute", "1 minute"),
                 col("ma_khgui"), col("ngay_nhap_may"),
               )
               .agg(
                 count("ma_khgui").alias("san_luong"),
                 sum("tong_tien").alias("doanh_thu")
               )
               .select(
                 col("ma_khgui"),
                 col("ngay_nhap_may"),
                 col("san_luong"),
                 col("doanh_thu")
               ).as[BaoCaoKhachHang]

    ds.writeStream
      .foreachBatch((dataSet: Dataset[BaoCaoKhachHang], batchId: Long) => {
        dataSet.write
               .format("jdbc")
               .options(postgresqlSinkOptions)
               .mode(SaveMode.Append)
               .save()
      })
      .start()
      .awaitTermination()

    ds.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate","false")
      .start()
      .awaitTermination()
  }
}