package com.ibm.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature._

object ModelCreation {
  def main(args: Array[String]): Unit = {

    val basedir = if (args.length > 0) args(0) else "/Users/josiahsams/SparkDemo/datasets/wiki.parquet"

    val sess = SparkSession.builder().appName("Demo").master("local[*]").getOrCreate()
    Logger.getRootLogger().setLevel(Level.ERROR)

    import sess.implicits._

    val wikiDF = sess.read.parquet(s"$basedir")
      .filter($"text".isNotNull)
      .select($"*", lower($"text").as("lowerText")).repartition(160)

    val tokenizer = new RegexTokenizer()
      .setInputCol("lowerText")
      .setOutputCol("words")
      .setPattern("\\W+")

    val is = ModelCreation.getClass.getResourceAsStream("/stopwords.txt")

    val stopwords = scala.io.Source.fromInputStream(is).getLines().toArray

    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("noStopWords").setStopWords(stopwords)

    val hashingTF = new HashingTF().setInputCol("noStopWords").setOutputCol("hashingTF").setNumFeatures(20000)

    val idf = new IDF().setInputCol("hashingTF").setOutputCol("idf")

    val normalizer = new Normalizer()
      .setInputCol("idf")
      .setOutputCol("features")

    val kmeans = new KMeans()
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setK(100)
      .setSeed(0) // for reproducability

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, remover, hashingTF, idf, normalizer, kmeans))

    val model = pipeline.fit(wikiDF)

    model.save(s"$basedir/../models/wiki.model")

  }

}
