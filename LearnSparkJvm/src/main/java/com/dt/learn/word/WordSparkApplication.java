package com.dt.learn.word;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author dt 2018/7/11 12:26
 */
public class WordSparkApplication {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("/tmp/sparkStudy/thatGirl.txt");
        JavaRDD<String> stringJavaRDD = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        JavaPairRDD<String, Integer> wordCount = stringJavaRDD.mapToPair(x -> {
            return new Tuple2<>(x, 1);
        }).reduceByKey((x, y) -> {
            return x + y;
        });

        wordCount.saveAsTextFile("/tmp/sparkStudy/word.text");

    }

}
