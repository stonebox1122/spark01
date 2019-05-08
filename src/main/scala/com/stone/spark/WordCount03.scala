package com.stone.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount03 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("yarn-client").setAppName("WordCount")
      .setJars(List("D:\\code\\IdeaProjects\\spark01\\target\\spark01-1.0-SNAPSHOT-jar-with-dependencies.jar"))
      .setIfMissing("spark.driver.host", "172.30.34.123")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.textFile("hdfs://172.30.60.60:8020/hdfs.log")
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .saveAsTextFile("hdfs://172.30.60.60:8020/out6")
    sparkContext.stop()
  }
}
