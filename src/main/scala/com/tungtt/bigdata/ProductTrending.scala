package com.tungtt.bigdata

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ProductTrending {

  def parseCsvRowUser(csvRow: String): Option[(Int, String, Int)] = {
    val parts = csvRow.split(",")
    if (parts.length >= 3) {
      try {
        val userId = parts(0).trim().toInt
        val fullName = parts(1).trim()
        val age = parts(2).trim().toInt
        if (userId > 0 && !fullName.isEmpty && age > 0) {
          return Some((userId, fullName, age))
        }
        None
      } catch {
        case _: Exception => None
      }
    } else {
      None
    }
  }

  def parseCsvRowProduct(csvRow: String): Option[(Int, String, Int)] = {
    val parts = csvRow.split(",")
    if (parts.length >= 3) {
      try {
        val productId = parts(0).trim().toInt
        val productName = parts(1).trim()
        val quantity = parts(2).trim().toInt
        if (productId > 0 && !productName.isEmpty && quantity > 0) {
          return Some((productId, productName, quantity))
        }
        None
      } catch {
        case _: Exception => None
      }
    } else {
      None
    }
  }

  def parseCsvRowWishList(csvRow: String): Option[(Int, Int)] = {
    val parts = csvRow.split(",")
    if (parts.length >= 2) {
      try {
        val userId = parts(0).trim().toInt
        val productId = parts(1).trim().toInt
        if (userId > 0 && productId > 0) {
          return Some((userId, productId))
        }
        None
      } catch {
        case _: Exception => None
      }
    } else {
      None
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                .master("local[1]")
                .appName("Product Trending")
                .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val rawUsers: RDD[String] = spark.sparkContext.textFile("data/csv/user.csv")
    val rawProducts: RDD[String] = spark.sparkContext.textFile("data/csv/product.csv")
    val rawWishList: RDD[String] = spark.sparkContext.textFile("data/csv/wishlist.csv")

    val users = rawUsers.map(csvRow => parseCsvRowUser(csvRow))
                .filter(data => data.isDefined)
    val products = rawUsers.map(csvRow => parseCsvRowProduct(csvRow))
                   .filter(data => data.isDefined)
    val wishlist = rawUsers.map(csvRow => parseCsvRowWishList(csvRow))
                   .filter(data => data.isDefined)

    val userDf = users.toDF("id", "name", "age")
    val productDf = users.toDF("id", "name", "quantity")
    val wishlistDf = users.toDF("user_id", "product_id")
  }
}
