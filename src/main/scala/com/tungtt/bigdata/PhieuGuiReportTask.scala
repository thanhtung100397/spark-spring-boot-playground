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

    val ds = df.selectExpr("CAST(value AS STRING)", "timestamp")
               .select(
                 from_json(col("value"), syncDataStruct).alias("value"),
                 col("timestamp")
               ).as[(SyncData[PhieuGui], Timestamp)]
               .map(tuple => {
                 val (syncData, timestamp) = tuple
                 val beforeData = syncData.payload.before
                 val afterData = syncData.payload.after
//                 if (afterData == null)
//                   // delete
//                 else if (beforeData == null)
//                   // insert
//                 else
//                   // update
                 (
                   syncData.payload.after.ma_khgui,
                   syncData.payload.after.ngay_nhap_may,
                   syncData.payload.after.tong_tien,
                   syncData.payload.after.tong_vat.toDecimal,
                   timestamp
                 )
               })
               .toDF("ma_khgui", "ngay_nhap_may", "tong_tien", "tong_vat", "timestamp")
//               .withWatermark("timestamp", "1 minute")
//               .groupBy(
//                 window(col("timestamp"), "1 minute", "1 minute"),
//                 col("ma_khgui"), col("ngay_nhap_may"),
//               )
//               .agg(
//                 count("ma_khgui").alias("san_luong"),
//                 sum(col("tong_tien").minus(col("tong_vat"))).alias("doanh_thu")
//               )
//               .select(
//                 col("ma_khgui"),
//                 col("ngay_nhap_may"),
//                 col("san_luong"),
//                 col("doanh_thu")
//               ).as[BaoCaoKhachHang]

//    val qs1 = ds.writeStream
//                .foreachBatch((dataSet: Dataset[BaoCaoKhachHang], batchId: Long) => {
//                  dataSet.write
//                         .format("jdbc")
//                         .options(postgresqlSinkOptions)
//                         .mode(SaveMode.Append)
//                         .save()
//                })
//                .start()

    val qs1 = ds.writeStream
                .foreachBatch((dataSet: Dataset[Row], batchId: Long) => {
                  val dbc: Connection = DriverManager.getConnection(
                    postgresqlSinkOptions("url"),
                    postgresqlSinkOptions("user"),
                    postgresqlSinkOptions("password"),
                  )

                  val totalRows = dataSet.count().intValue();

                  val querySt = dbc.prepareStatement(
                    s"INSERT INTO ${postgresqlSinkOptions("dbtable")} (ma_khgui, ngay_nhap_may, tong_tien, tong_vat) " +
                         s"VALUES ${List.fill(totalRows)("(?,?,?,?)").mkString(",")} " +
                         s"ON DUPLICATE KEY CONFLICT (ma_khgui, ngay_nhap_may) DO UPDATE SET " +
                         s"tong_tien = tong_tien + EXCLUDED.tong_tien, " +
                         s"tong_vat = tong_vat + EXCLUDED.tong_vat"
                  )

                  var idx = 1;
                  dataSet.foreach(row => {
                    querySt.setString(idx++, row.getAs[String]("ma_khgui"))
                  })

//                  dataSet.write
//                         .format("jdbc")
//                         .options(postgresqlSinkOptions)
//                         .mode(SaveMode.Overwrite)
//                         .save()
                })
                .start()

    val qs2 = ds.writeStream
                .outputMode("append")
                .format("console")
                .start()

    qs1.awaitTermination()
    qs2.awaitTermination()
  }
}