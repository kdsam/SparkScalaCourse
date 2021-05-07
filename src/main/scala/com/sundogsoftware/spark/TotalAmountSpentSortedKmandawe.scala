package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark._

object TotalAmountSpentSortedKmandawe {

  def parseLine(line: String): (Int, Float) = {
    val fields = line.split(",")
    val custId = fields(0).toInt
    val amount = fields(2).toFloat
    (custId, amount)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "TotalAmountSpentSortedKmandawe")
    val lines = sc.textFile("data/customer-orders.csv")

    val customerAndAmount = lines.map(parseLine)
    val customerAmountSpent = customerAndAmount.reduceByKey( (x, y) => x + y)
    val customerAmountSpentFlipped = customerAmountSpent.map(x => (x._2, x._1))
    val sorted = customerAmountSpentFlipped.sortByKey()
    val results = sorted.collect()
    for (results <- results) {
      println(f"(${results._2}, ${results._1}%.2f)")
    }
  }

}
