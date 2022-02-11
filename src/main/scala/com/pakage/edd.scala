package com.pakage
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
object csvread {
  def main(args :Array[String]):Unit = {
    // Lib for avoiding the errors
    import org.apache.log4j._
    Logger.getLogger("org").setLevel((Level.ERROR))
    // making a spark session
    val spark: SparkSession = SparkSession.builder
      .appName("test")
      .master("local[2]")
      .getOrCreate()
     // declaring variables for accesing MySQL DB
        val database = "classicmodels"
        val user = "root"
        val password = "Saad786b"
        val connString = "jdbc:mysql://localhost:3306/" + database
        // Reading The Data Base
        val  orders = (spark.read.format("jdbc")
          .option("url", connString)
          .option("dbtable", "orders")
          .option("user", user)
          .option("password", password)
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .load())
        val  customers = (spark.read.format("jdbc")
          .option("url", connString)
          .option("dbtable", "customers")
          .option("user", user)
          .option("password", password)
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .load())
        val  payments = (spark.read.format("jdbc")
          .option("url", connString)
          .option("dbtable", "payments")
          .option("user", user)
          .option("password", password)
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .load())
    // Transformations
    val df = orders.join(customers,orders("customerNumber") ===  customers("customerNumber"),"inner").
                    join(payments,payments("customerNumber") ===  customers("customerNumber"),"inner").
                    withColumn("originalPrice",lit(payments("amount")*2)).
                    filter(payments("amount") > 1000 && orders("status") === "Shipped").
                    select("customerName","shippedDate","status","amount","originalPrice","country")
    df.printSchema()
    df.show()
    df.write.format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/" + "target")
      .option("dbtable", "customerDetail")
      .option("user", "root")
      .option("password", "Saad786b")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .mode(SaveMode.Overwrite)
      .save()
  }
}