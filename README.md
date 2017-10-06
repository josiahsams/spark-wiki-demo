# spark-wiki-demo

This repository contains code to parse & convert the Wikidump files into parquet files ready to be consumed by Apache Spark programs.

* File: [pageviewRequest.scala](https://github.com/josiahsams/spark-wiki-demo/blob/master/src/main/scala/com/ibm/demo/pageviewRequest.scala)

This data dump is taken from Wikimedia site : https://dumps.wikimedia.org/other/pageviews/2017/
It holds daily consolidated traffic data based on the article requests.
Data is found is space separated colums in human readable format
Note: preprocessing is done to filter out only EN articles and saved them in parquet format


* File: [pageviewSec.scala](https://github.com/josiahsams/spark-wiki-demo/blob/master/src/main/scala/com/ibm/demo/pageviewSec.scala)

This file operates on data downloaded from,
https://old.datahub.io/en/dataset/english-wikipedia-pageviews-by-second
This file contains a count of pageviews to the English-language Wikipedia from 2015-03-16T00:00:00 to 2015-04-25T15:59:59, grouped by timestamp (down to a one-second resolution level) and site (mobile or desktop).

* File [ProcessWikiPage.scala](https://github.com/josiahsams/spark-wiki-demo/blob/master/src/main/scala/com/ibm/demo/ProcessWikiPage.scala)

This is used to process data downloaded from,
https://dumps.wikimedia.org/wikidatawiki/20171001/, 
https://dumps.wikimedia.org/wikidatawiki/20171001/wikidatawiki-20171001-pages-articles.xml.bz2
This is a snapshot data of wikipedia as on 01 Oct 2017

* File [ModelCreation.scala](https://github.com/josiahsams/spark-wiki-demo/blob/master/src/main/scala/com/ibm/demo/ModelCreation.scala)

This is used to create KMeans Model on wikipedia data processed by the previous script.
