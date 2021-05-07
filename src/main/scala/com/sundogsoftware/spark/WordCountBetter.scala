package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext


object WordCountBetter {

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCountBetter")

    // Read each line of my book into an RDD
    val input = sc.textFile("data/book.txt")

    // Split into words separated by a space character
    val words = input.flatMap(x => x.split("\\W+"))

    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())

    // Count up the occurrences of each word
    val wordCounts = lowercaseWords .countByValue()

    // Print the results.
    wordCounts.foreach(println)

  }

}
