package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object TotalAmountSpentKmandawe {

  def parseLine(line: String): (Int, Float) = {
    val fields = line.split(",")
    val custId = fields(0).toInt
    val amount = fields(2).toFloat
    (custId, amount)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "TotalAmountSpentKmandawe")
    val lines = sc.textFile("data/customer-orders.csv")

    val customerAndAmount = lines.map(parseLine)
    val customerAmountSpent = customerAndAmount.reduceByKey( (x, y) => x + y)
    val results = customerAmountSpent.collect()
    for (results <- results) {
      println(f"(${results._1}, ${results._2}%.2f)")
    }
  }

}
