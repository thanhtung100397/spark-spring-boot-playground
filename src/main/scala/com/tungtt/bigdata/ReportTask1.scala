package com.tungtt.bigdata

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ReportTask1 {

  def parseDataRawRow(rawDataRow: String): String = {
    val rawDataParts = rawDataRow.split("\"")
    if (rawDataParts.length > 1) {
      rawDataParts(1)
    } else {
      ""
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                .master("local[1]")
                .appName("Report Task 1")
                .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val rawDataRdd: RDD[String] = spark.sparkContext.textFile("data/Marvel-names.txt")

    val dataRdd: RDD[String] = rawDataRdd.map(rawDataRow => parseDataRawRow(rawDataRow))
                               .filter(data => !data.isEmpty)

    val heroDF: DataFrame = dataRdd.toDF("hero_name")
    val heroTotalDisplayDS: Dataset[Row] = heroDF.groupBy("hero_name")
                                           .agg(
                                                 count("hero_name").as("total_number")
                                               )
                                           .sort(desc("total_number"))
    heroTotalDisplayDS.show()
    println(heroTotalDisplayDS.count())

    heroTotalDisplayDS.write
      .format("jdbc")
      .mode("overwrite")
      .option("url", "jdbc:postgresql:///test")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "test_report")
      .option("user", "")
      .option("password", "")
      .save()
  }
}
