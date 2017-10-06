package com.ibm.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Demo
 *
 */
object ProcessWikiPage {

  // UDF
  def processText(text: String): String = {
    text.split("\\W").filter(row => row.size > 2 && row.matches("[A-Za-z]+")).mkString(" ")
  }

  def main(args: Array[String]): Unit = {

    val basedir = if (args.length > 0) args(0) else "/Users/josiahsams/samplexml"
    val lastDays = if (args.length > 1) args(1).toInt else 10

    val sess = SparkSession.builder().appName("Demo").master("local[*]").getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)

    import sess.implicits._

    val df = sess.sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "page")
      .load(s"$basedir")
      .filter($"text".isNotNull)
      .repartition(16)

    val processTextUDF = sess.sqlContext.udf.register("processText", (s: String) => processText(s))

    val selCols = df.select($"id", $"title", $"revision.id".as("revid"),
      $"revision.timestamp".cast(TimestampType).as("lastrev_pdt_time"),
      $"revision.comment._VALUE".as("comment"),
      $"revision.contributor.id".as("contributorid"),
      $"revision.contributor.username".as("contributorusername"),
      $"revision.contributor.ip".as("contributorip"),
      processTextUDF($"revision.text._VALUE").as("text")
    )

    val filterCols = selCols
      .filter($"lastrev_pdt_time" >= date_sub(current_timestamp(), lastDays))


    filterCols.printSchema()

    filterCols.write.format("parquet").mode(SaveMode.Overwrite)
      .save(s"$basedir/../wiki.parquet")

    val pdf = sess.read.parquet(s"${basedir}/../wiki.parquet")
    pdf.show

  }

}
