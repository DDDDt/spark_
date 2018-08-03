package com.dt.learn.word.rdd

import com.dt.learn.SparkContextScalaApplication
import org.apache.spark.rdd.RDD

/**
  * @author dt 2018/7/15 19:49
  */
object CreateRdd {

  /**
    * 通过parallelize创建一个简单的RDD
    * @return
    */
  def createRddForSimple():RDD[Int]={
    val sc = SparkContextScalaApplication.createSparkContextForConf
    val rdd = sc.parallelize(List(1,2,3,4,5,6,7))
    rdd
  }

}
