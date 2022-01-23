package com.tungtt.bigdata

import com.tungtt.bigdata.models.{DebeziumDecimal, SyncData}
import org.apache.spark.sql.{Dataset, Encoders, Row, SaveMode, SparkSession}

import java.sql.{Connection, Date, DriverManager, Timestamp}

object PhieuGuiReportTask {

  case class PhieuGui(var ma_khgui: String,
                         var ngay_nhap_may: Date,
                         var tong_tien: BigDecimal,
                         var tong_vat: DebeziumDecimal) {

  }

  case class BaoCaoKhachHang(var ma_khgui: String,
                             var ngay_nhap_may: String,
                             var san_luong: Long,
                             var doanh_thu: BigDecimal) {

  }

  val postgresqlSinkOptions: Map[String, String] = Map(
    "dbtable" -> "test",
    "createTableColumnTypes" -> "ma_khgui VARCHAR(36), ngay_nhap_may DATE,INTEGER, doanh_thu DECIMAL, PRIMARY KEY (ma_khgui, ngay_nhap_may)",
    "user" -> "",
    "password" -> "",
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://:5432/postgres"
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                            .master("local[1]")
                            .appName("PhieuGuiReportTask")
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

    val ds = df.selectExpr("CAST(value AS STRING)", "timestamp")
               .select(
                 from_json(col("value"), syncDataStruct).alias("value"),
                 col("timestamp")
               ).as[(SyncData[PhieuGui], Timestamp)]
               .map(tuple => {
                 val (syncData, timestamp) = tuple
                 val oldData = syncData.payload.before
                 val newData = syncData.payload.after
                 if (newData == null)
                   // delete
                   (
                     newData.ma_khgui,
                     newData.ngay_nhap_may,
                     -1,
                     -newData.tong_tien,
                     -newData.tong_vat.toDecimal,
                     timestamp
                   )
                 else if (oldData == null) {
                   // insert
                   (
                     newData.ma_khgui,
                     newData.ngay_nhap_may,
                     1,
                     newData.tong_tien,
                     newData.tong_vat.toDecimal,
                     timestamp
                   )
                 } else
                   //update
                   (
                     newData.ma_khgui,
                     newData.ngay_nhap_may,
                     0,
                     newData.tong_tien,
                     newData.tong_vat.toDecimal,
                     timestamp
                   )
               })
               .toDF(
                 "ma_khgui", "ngay_nhap_may", "counter", "tong_tien", "tong_vat", "timestamp"
               )
               .withWatermark("timestamp", "5 minutes")
               .groupBy(
                 window(col("timestamp"), "1 day", "1 day"),
                 col("ma_khgui"), col("ngay_nhap_may"),
               )
               .agg(
                 sum(col("counter")).alias("san_luong"),
                 sum(col("tong_tien").minus(col("tong_vat"))).alias("doanh_thu")
               )
               .select(
                 col("ma_khgui"),
                 date_format(col("ngay_nhap_may"), "yyyy-MM-dd").alias("ngay_nhap_may"),
                 col("san_luong"), col("doanh_thu")
               ).as[BaoCaoKhachHang]


    val qs1 = ds.map(data => {
      (

      )
        s"INSERT INTO ${postgresqlSinkOptions("dbtable")} (ma_khgui, ngay_nhap_may, san_luong, doanh_thu) " +
        s"VALUES ('${data.ma_khgui}', '${data.ngay_nhap_may}', ${data.san_luong}, ${data.doanh_thu}) " +
        s"ON CONFLICT (ma_khgui, ngay_nhap_may) DO UPDATE SET " +
        s"san_luong = ${postgresqlSinkOptions("dbtable")}.san_luong + EXCLUDED.san_luong, " +
        s"doanh_thu = ${postgresqlSinkOptions("dbtable")}.doanh_thu + EXCLUDED.doanh_thu "
      })
      .writeStream
      .outputMode("complete")
      .foreachBatch((data: Dataset[String], batchId: Long) => {
        data.coalesce(1)
            .write
            .mode("append")
            .format("text")
            .text("data/bao_cao/output")
      })
      .start()

//    val qs1 = ds.writeStream
//                .foreachBatch((dataSet: Dataset[BaoCaoKhachHang], batchId: Long) => {
//                  dataSet.write
//                         .format("jdbc")
//                         .options(postgresqlSinkOptions)
//                         .mode(SaveMode.Overwrite)
//                         .save()
//                })
//                .start()

//    val qs1 = ds.writeStream
//                .foreachBatch((dataSet: Dataset[Row], batchId: Long) => {
//                  val dbc: Connection = DriverManager.getConnection(
//                    postgresqlSinkOptions("url"),
//                    postgresqlSinkOptions("user"),
//                    postgresqlSinkOptions("password"),
//                  )
//                })
//                .start()

    val qs2 = ds.writeStream
                .outputMode("complete")
                .option("truncate", value = false)
                .format("console")
                .start()

    qs1.awaitTermination()
    qs2.awaitTermination()
  }
}