/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ufpel.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author WeslenSchiavon
 */
public class Sql {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("BusProcessor");

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config(conf)
                .getOrCreate();

        Dataset<Row> text = spark.read().text("stops.txt");

        text.createOrReplaceTempView("dados");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM dados");
        sqlDF.show();

        //text.select("value").show();
    }
}
