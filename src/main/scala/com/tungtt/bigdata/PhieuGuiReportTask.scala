package com.tungtt.bigdata

import com.tungtt.bigdata.models.{DebeziumDecimal, SyncData}
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}

import java.sql.{Date, Timestamp}

object PhieuGuiReportTask {

  case class PhieuGuiRaw(var ma_khgui: String,
                         var ngay_nhap_may: Date,
                         var tong_tien: BigDecimal,
                         var tong_vat: DebeziumDecimal) {

  }

  case class PhieuGui(var ma_khgui: String,
                      var ngay_nhap_may: Date,
                      var tong_tien: BigDecimal,
                      var tong_vat: BigDecimal,
                      var timestamp: Timestamp) {

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

    val syncDataStruct = Encoders.product[SyncData[PhieuGuiRaw]].schema

    val ds = df.selectExpr("CAST(value AS STRING)", "timestamp")
               .select(
                 from_json(col("value"), syncDataStruct).alias("value"),
                 col("timestamp")
               ).as[(SyncData[PhieuGuiRaw], Timestamp)]
               .map(tuple => {
                 val (syncData, timestamp) = tuple
                 (
                   syncData.payload.after.ma_khgui,
                   syncData.payload.after.ngay_nhap_may,
                   syncData.payload.after.tong_tien,
                   syncData.payload.after.tong_vat.toDecimal,
                   timestamp
                 )
               })
               .toDF("ma_khgui", "ngay_nhap_may", "tong_tien", "tong_vat", "timestamp")
               .withWatermark("timestamp", "1 minute")
               .groupBy(
                 window(col("timestamp"), "1 minute", "1 minute"),
                 col("ma_khgui"), col("ngay_nhap_may"),
               )
               .agg(
                 count("ma_khgui").alias("san_luong"),
                 sum(col("tong_tien").minus(col("tong_vat"))).alias("doanh_thu")
               )
               .select(
                 col("ma_khgui"),
                 col("ngay_nhap_may"),
                 col("san_luong"),
                 col("doanh_thu")
               ).as[BaoCaoKhachHang]

//    val qs1 = ds.writeStream
//                .foreachBatch((dataSet: Dataset[BaoCaoKhachHang], batchId: Long) => {
//                  dataSet.write
//                         .format("jdbc")
//                         .options(postgresqlSinkOptions)
//                         .mode(SaveMode.Append)
//                         .save()
//                })
//                .start()

    val qs2 = ds.writeStream
                .outputMode("complete")
                .format("console")
                .option("truncate", "false")
                .start()

//    qs1.awaitTermination()
    qs2.awaitTermination()
  }
}