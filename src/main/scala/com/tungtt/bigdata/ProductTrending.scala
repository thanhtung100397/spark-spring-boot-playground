package com.tungtt.bigdata

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ProductTrending {

  def parseCsvRowUser(csvRow: String): (Int, String, Int) = {
    val parts = csvRow.split(",")
    if (parts.length >= 3) {
      try {
        val userId = parts(0).trim().toInt
        val fullName = parts(1).trim()
        val age = parts(2).trim().toInt
        if (userId > 0 && fullName.nonEmpty && age > 0) {
          return (userId, fullName, age)
        }
        null
      } catch {
        case _: Exception => null
      }
    } else {
      null
    }
  }

  def parseCsvRowProduct(csvRow: String): (Int, String, Int) = {
    val parts = csvRow.split(",")
    if (parts.length >= 3) {
      try {
        val productId = parts(0).trim().toInt
        val productName = parts(1).trim()
        val quantity = parts(2).trim().toInt
        if (productId > 0 && productName.nonEmpty && quantity > 0) {
          return (productId, productName, quantity)
        }
        null
      } catch {
        case _: Exception => null
      }
    } else {
      null
    }
  }

  def parseCsvRowWishList(csvRow: String): (Int, Int) = {
    val parts = csvRow.split(",")
    if (parts.length >= 2) {
      try {
        val userId = parts(0).trim().toInt
        val productId = parts(1).trim().toInt
        if (userId > 0 && productId > 0) {
          return (userId, productId)
        }
        null
      } catch {
        case _: Exception => null
      }
    } else {
      null
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
                .filter(data => data != null)
    val products = rawProducts.map(csvRow => parseCsvRowProduct(csvRow))
                   .filter(data => data != null)
    val wishlist = rawWishList.map(csvRow => parseCsvRowWishList(csvRow))
                   .filter(data => data != null)

    val userDf = users.toDF("id", "name", "age")
    val productDf = products.toDF("id", "name", "quantity")
    val wishlistDf = wishlist.toDF("user_id", "product_id")

    userDf.show();
    productDf.show();
    wishlistDf.show();

    userDf.as("u")
      .join(wishlistDf.as("wl"), col("u.id") === col("wl.user_id"), "inner")
      .join(productDf.as("p"), col("wl.product_id") === col("p.id"), "inner")
      .groupBy(col("p.id"))
      .agg(
        first("p.name").as("name"),
        first("p.quantity").as("quantity"),
        count("p.id").as("total_wished")
      )
      .show()
  }
}
