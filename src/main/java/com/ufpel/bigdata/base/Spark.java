/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ufpel.bigdata.base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    private final JavaSparkContext sparkContext;

    public Spark(String log) {
        this.log = log;

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Spark Master");
        this.sparkContext = new JavaSparkContext(conf);

        this.data = this.sparkContext.textFile(log);
    }

    public JavaPairRDD<String, Integer> ContaPalavras(String separador) {

        JavaPairRDD<String, Integer> counts = this.data
                .flatMap(x -> Arrays.asList(x.split(separador)).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        return counts;

    }

    public double calcPiPorTequinicaDosDardos(int numIteracoes) {

        List<Integer> l = new ArrayList<>(numIteracoes);
        for (int i = 0; i < numIteracoes; i++) {
            l.add(i);
        }

        long count = this.sparkContext.parallelize(l).filter(i -> {
            double x = Math.random();
            double y = Math.random();
            return x * x + y * y < 1;
        }).count();

        return 4.0 * count / numIteracoes;
    }

    public JavaSparkContext getSparkContext() {
        return sparkContext;
    }

}
