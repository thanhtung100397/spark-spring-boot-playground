package com.tungtt.bigdata

import com.tungtt.bigdata.models.{PhieuGui, SyncData}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
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
               .select(from_json($"value", phieuGuiStruct).as[SyncData[PhieuGui]])
               .map(syncData => syncData.payload.after)

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