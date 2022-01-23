package com.tungtt.bigdata

import org.apache.spark.sql.{ ForeachWriter, Row, SparkSession}

import java.sql.{Connection, DriverManager, Statement}

object DatabaseWriteTask {

  val postgresqlSinkOptions: Map[String, String] = Map(
    "dbtable" -> "test",
    "createTableColumnTypes" -> "ma_khgui VARCHAR(36), ngay_nhap_may DATE, san_luong INTEGER, doanh_thu DECIMAL, PRIMARY KEY (ma_khgui, ngay_nhap_may)",
    "user" -> "",
    "password" -> "",
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://:5432/postgres"
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                            .master("local[1]")
                            .appName("DatabaseWriteTask")
                            .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = spark.readStream
                  .text("data/bao_cao/output")

    val qs1 = df.writeStream
                .foreach(new ForeachWriter[Row] {
                  var dbc: Connection = _
                  var statement: Statement = _

                  def open(partitionId: Long, version: Long): Boolean = {
                    dbc = DriverManager.getConnection(
                      postgresqlSinkOptions("url"),
                      postgresqlSinkOptions("user"),
                      postgresqlSinkOptions("password"),
                    )
                    dbc.setAutoCommit(false);
                    statement = dbc.createStatement()
                    true
                  }

                  def process(record: Row) = {
                    val query = record.getAs[String](0)
                    println(query)
                    statement.addBatch(query);
                  }

                  def close(errorOrNull: Throwable): Unit = {
                    statement.executeBatch()
                    dbc.commit()
                    dbc.close()
                  }
                })
                .start()


    val qs2 = df.writeStream
                .outputMode("append")
                .format("console")
                .start()

    qs1.awaitTermination()
    qs2.awaitTermination()
  }
}
