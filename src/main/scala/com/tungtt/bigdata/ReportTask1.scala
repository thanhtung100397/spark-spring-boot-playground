package com.tungtt.bigdata

import org.apache.spark.sql.SparkSession

object ReportTask1 {

  def parseDataRawRow(rawDataRow: String) = {
    val rawDataParts = rawDataRow.split("\"")
    if (rawDataParts.length > 1) {
      rawDataParts(1)
    } else {
      None
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Report Task 1")
      .getOrCreate()

    val rawDataRdd = spark.sparkContext.textFile("data/Marvel-names.txt")

    val dataRdd = rawDataRdd.map(rawDataRow => parseDataRawRow(rawDataRow))
      .filter(data => data != None)

    println(dataRdd.count())

//    val db = spark.read
//      .format("jdbc")
//      .option("url", "jdbc:postgresql://:/")
//      .option("driver", "org.postgresql.Driver")
//      .option("dbtable", "")
//      .option("user", "")
//      .option("password", "")
//      .load()
//
//    println(db)
  }
}
