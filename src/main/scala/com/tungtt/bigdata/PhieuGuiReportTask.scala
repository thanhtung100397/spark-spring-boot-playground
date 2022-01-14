package com.tungtt.bigdata

import com.tungtt.bigdata.models.{PhieuGui, SyncData}
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType, StringType, StructType}

import java.math.BigInteger

object PhieuGuiReportTask {

  val phieuGuiStruct: StructType = new StructType()
    .add("payload", new StructType()
      .add("after", new StructType()
        .add("ma_phieugui", StringType, nullable = true)
        .add("ma_khgui", StringType, nullable = true)
        .add("ngay_nhap_may", StringType, nullable = true)
        .add("tong_tien", DecimalType(0,9), nullable = true)
        .add("tong_vat", new StructType()
          .add("scale", IntegerType, nullable = true)
          .add("value", StringType, nullable = true)
          ,
        nullable = true)
        ,
      nullable = false)
      .add("source", new StructType()
        .add("ts_ms", LongType, nullable = true)
        ,
        nullable = false)
      ,
    nullable = false)

  case class PhieuGui(var ma_phieugui: String,
                      var ma_khgui: String,
                      var ngay_nhap_may: String,
                      var tong_tien: BigDecimal,
                      var tong_vat: BigDecimal,
                      var timestamp: Long) {

  }

  val postgresqlSinkOptions: Map[String, String] = Map(
    "dbtable" -> "phieu_gui_kafka",
    "user" -> "",
    "password" -> "",
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://:5432/postgres"
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

    val ds = df.selectExpr("CAST(value AS STRING)").as[String]
               .select(from_json($"value", phieuGuiStruct).alias("value"))
               .map(syncData => {
                 println(syncData.get(0).getClass);
                 PhieuGui("1", "a", "a", BigDecimal(1), BigDecimal(2), 1234567)
               })
//               .groupBy(
//                 window($"timestamp", "1 minutes", "5 minutes"),
//                 col("ma_phieugui")
//               )
//               .agg(Map(
//                 "ma_phieugui" -> "count",
//               ))

//    ds.writeStream
//      .foreachBatch((dataSet: Dataset[PhieuGui], batchId: Long) => {
//        dataSet.write
//               .format("jdbc")
//               .options(postgresqlSinkOptions)
//               .mode(SaveMode.Append)
//               .save()
//      })
//      .start()
//      .awaitTermination()

    ds.writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()
  }
}