package com.ibm.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

/**
  * Created by joe on 9/25/17.
  */
object pageviewRequest {
  case class pagecount(project: Option[String], article: Option[String]
                       ,requests: Option[Int], hour_vise: Option[String])
  def main(args: Array[String]): Unit = {

    Logger.getRootLogger().setLevel(Level.ERROR)
    val sess = SparkSession.builder()
      .master("local[*]")
      .appName("DatasetConvertor")
      .getOrCreate()

    import sess.implicits._

    val rawData = sess.read
        .option("delimiter", " ")
        .option("inferSchema", true)
        .csv("/Users/josiahsams/SparkDemo/datasets/pageviews-20170925-110000").cache

    val cols = Seq("project", "article", "requests", "hour_vise")
    val rawDF = rawData.toDF(cols:_*)
    val ds = rawDF.as[pagecount]

    println(s"Num partitions : ${ds.rdd.getNumPartitions}")
    ds.printSchema()
    ds.show()
    val enDS = ds.filter(_.project.getOrElse("").startsWith("en")).repartition(4).select("project", "article", "requests")

    enDS.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/Users/josiahsams/SparkDemo/datasets/pageviews.parquet")
  }
}
