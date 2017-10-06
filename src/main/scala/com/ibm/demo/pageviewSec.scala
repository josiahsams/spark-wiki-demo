package com.ibm.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by joe on 9/27/17.
  */
object pageviewSec {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger().setLevel(Level.ERROR)
    val sess = SparkSession.builder()
      .master("local[*]")
      .appName("Data Convertor")
      .getOrCreate()

    val rawDF = sess.read
        .option("delimiter", "\t")
        .option("inferSchema", true)
        .option("header", true)
        .csv("/Users/josiahsams/SparkDemo/datasets/pageviews-by-second-tsv").cache

    rawDF.printSchema()
    rawDF.show()

    val partDF = rawDF.repartition(5)
    partDF.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save("/Users/josiahsams/SparkDemo/datasets/pageviews-by-second-tsv.csv")

  }

}
