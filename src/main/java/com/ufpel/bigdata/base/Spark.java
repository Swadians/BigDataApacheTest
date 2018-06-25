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

    private JavaRDD<String> data;
    private final JavaSparkContext sparkContext;

    public Spark() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Spark Master");
        conf.set("spark.driver.maxResultSize", "6g");
        conf.set("spark.network.timeout", "7000s");
        conf.set("spark.executor.heartbeatInterval", "6000s");

        this.sparkContext = new JavaSparkContext(conf);
    }

    public JavaPairRDD<String, Integer> ContaPalavras(String separador, String arquivo) {
        this.data = this.sparkContext.textFile(arquivo);

        JavaPairRDD<String, Integer> counts = this.data
                .flatMap(x -> Arrays.asList(x.split(separador)).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        return counts;

    }

    public double calcPiPorTequinicaDosDardos(int numIteracoes) {
        List<Boolean> l = new ArrayList<>(numIteracoes);
        for (int i = 0; i < numIteracoes; i++) {
            l.add(true);
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

    public void close() {
        sparkContext.close();
    }
}
