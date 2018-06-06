/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ufpel.bigdata.base;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author WeslenSchiavon
 */
public class Spark {

    private final String log;
    private final JavaRDD<String> data;

    public Spark(String log) {
        this.log = log;

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Spark Master");
        JavaSparkContext ctx = new JavaSparkContext(conf);

        this.data = ctx.textFile(log);
    }

    public JavaPairRDD<String, Integer> ContaPalavras(String separador) {

        JavaPairRDD<String, Integer> counts = this.data
                .flatMap(x -> Arrays.asList(x.split(separador)).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        return counts;

    }
}
