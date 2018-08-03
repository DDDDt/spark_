package com.dt.learn.word.rdd

/**
  * @author dt 2018/7/19 12:22
  */
class RddOperation {

  def rdd2Map(): Unit={

    val rdd = CreateRdd.createRddForSimple();
    val rdd2 = rdd.map(x => x*x)
    rdd2.foreach(println _)

  }

}
