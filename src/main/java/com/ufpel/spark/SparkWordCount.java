/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ufpel.spark;

import com.ufpel.bigdata.base.Spark;
import com.ufpel.util.Monitorador;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;

/**
 *
 * @author WeslenSchiavon
 */
public class SparkWordCount {

    public static void main(String[] args) {
        Monitorador.startMemoryMonitor();
        Monitorador.startTimeMonitoring();

        Spark spark = new Spark("stops.txt");

        JavaPairRDD<String, Integer> ContaPalavras = spark.ContaPalavras(" ");

        System.out.println("Tempo: " + Monitorador.getTimeExecutation() + "s");
        System.out.println("Uso de momoria: " + Monitorador.getMaxMemoryUsage() + "MB");

        Map<String, Integer> wordMap = ContaPalavras.collectAsMap();

        wordMap.keySet().parallelStream().forEach(palavra -> System.out.println("Foram encontradas " + wordMap.get(palavra) + " ocorrencias para palavra " + palavra));
    }
}
