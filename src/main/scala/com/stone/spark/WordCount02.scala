package com.stone.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount02 extends App{
  val sparkConf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("WordCount")
  val sparkContext = new SparkContext(sparkConf)
  sparkContext.textFile("hdfs://172.30.60.60:8020/hdfs.log")
    .flatMap(_.split(" "))
    .map((_,1))
    .reduceByKey(_+_)
    .saveAsTextFile("hdfs://172.30.60.60:8020/out1")
  sparkContext.stop()
}
