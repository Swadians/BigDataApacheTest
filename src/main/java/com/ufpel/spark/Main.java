/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ufpel.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 *
 * @author WeslenSchiavon
 */
public class Main {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("BusProcessor");
        JavaSparkContext ctx = new JavaSparkContext(conf);

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config(conf)
                .getOrCreate();

        spark.read().text("stops.txt");

        // configuração do Spark
        // carrega os dados dos ônibus de sp
        JavaRDD<String> linhas = ctx.textFile("stops.txt");

        linhas.map(entrada -> entrada.split(" ")).count();

        JavaRDD<String> registroFiltrados = linhas.filter(registro -> registro.contains("Vila"));

        JavaPairRDD<String, Integer> mapToPair = registroFiltrados.mapToPair(registro -> new Tuple2<>(registro.split(",")[1], 1));

        // escreve o número de ônibus que existem no arquivo
        System.out.println("####" + mapToPair.reduceByKey((x, y) -> {
            System.out.println("x" + x);
            System.out.println("y" + y);
            return x + y;
        }).collect().size());

        ctx.close();
    }
}
