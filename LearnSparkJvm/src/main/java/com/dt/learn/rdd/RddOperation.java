package com.dt.learn.rdd;

import org.apache.spark.api.java.JavaRDD;

/**
 * @author dt 2018/7/18 12:17
 * rdd 的一些转化操作 lazy
 */
public class RddOperation {

    public void rddTransOperation(){

        var createRdd = new CreateRdd();
        JavaRDD<Integer> rddForSimple = createRdd.createRddForSimple();

        JavaRDD<Integer> filter = rddForSimple.filter(x -> x > 2);

        filter.take(10).forEach(x -> System.out.println(x));

    }

    public void rddMap2(){

        CreateRdd createRdd = new CreateRdd();
        JavaRDD<Integer> rddForSimple = createRdd.createRddForSimple();
        JavaRDD<Integer> map = rddForSimple.map(x -> x * x);
        map.partitions().forEach(x -> System.out.println(x));

    }

}
