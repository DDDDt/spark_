package com.dt.learn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author dt 2018/7/3 12:21
  */
object SparkContextScalaApplication {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("my scala app")
    val sc = new SparkContext(conf)

    /*关闭*/
    sc.stop()

  }

}
