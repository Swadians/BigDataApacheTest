/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ufpel.spark;

import com.ufpel.bigdata.base.Spark;
import com.ufpel.util.Monitorador;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;

/**
 *
 * @author WeslenSchiavon
 */
public class SparkWordCount {

    public static void main(String[] args) throws FileNotFoundException {
        PrintStream ps = new PrintStream(new File("Relatorio.txt"));

        Monitorador.startMemoryMonitor();
        Monitorador.startTimeMonitoring();

        System.out.println("Lendo arquivo: " + args[0]);
        Spark spark = new Spark(args[0]);

        JavaPairRDD<String, Integer> ContaPalavras = spark.ContaPalavras(" ");

        System.out.println("Coletando dados....");
        Map<String, Integer> wordMap = ContaPalavras.collectAsMap();

        ps.println("Tempo: " + Monitorador.getTimeExecutation() + " minutos");
        ps.println("Uso de momoria: " + Monitorador.getMaxMemoryUsage() + "MB");

        ps.close();

        System.out.println("Imprimindo:");

        System.out.println("Numero de palavras lidas: " + wordMap.keySet().size());

        //  wordMap.keySet().parallelStream().forEach(palavra -> System.out.println("Foram encontradas " + wordMap.get(palavra) + " ocorrencias para palavra " + palavra));
    }
}
