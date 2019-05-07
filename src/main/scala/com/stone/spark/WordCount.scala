package com.stone.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App{
  val sparkContext = new SparkContext(new SparkConf().setAppName("WordCount"))
  sparkContext.textFile("hdfs://nameservice1/hdfs.log")
    .flatMap(_.split(" "))
    .map((_,1))
    .reduceByKey(_+_)
    .saveAsTextFile("hdfs://nameservice1/out3")
  sparkContext.stop()
}
