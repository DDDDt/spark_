package com.dt.learn.rdd;

import com.dt.learn.SparkContextApplication;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @author dt 2018/7/15 20:04
 */
public class CreateRdd {

    /**
     * 使用简单的方式创建RDD
     * @return
     */
    public JavaRDD<Integer> createRddForSimple(){

        JavaSparkContext javaSparkContext = SparkContextApplication.crateSparkContextFactory();
        JavaRDD<Integer> rdd = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
        return rdd;

    }

}
