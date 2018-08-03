package com.dt.learn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author dt 2018/7/2 19:49
 * java 创建 spark
 */
public class SparkContextApplication {

    public static JavaSparkContext crateSparkContextFactory(){

        /**
         * 创建最简单基本的 sparkContext
         * setMaster 集群的 url 告诉 spark 需要连接的集群地址，使用 local 可以让 spark 运行在单线程上而无需连接到集群
         * appname 运用名
         */
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        return javaSparkContext;
    }

}
