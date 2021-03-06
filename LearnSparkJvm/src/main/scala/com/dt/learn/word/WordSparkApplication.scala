package com.dt.learn.word

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author dt 2018/7/15 18:28
  */
object WordSparkApplication {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wordCount")
    val sc = new SparkContext(conf)
    val input = sc.textFile("/tmp/sparkStudy/thatGirl.txt")
    val words = input.flatMap(x => x.split(" "))
    val counts = words.map(word => (word,1)).reduceByKey((v1,v2) => v1+v2)
    /*保存到缓存中*/
    counts.persist()
    counts.saveAsTextFile("/tmp/sparkStudy/word")
    /*清楚缓存中的数据*/
    counts.unpersist()
    sc.stop()

  }

}
