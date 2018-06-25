/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ufpel.spark;

import com.ufpel.bigdata.base.Spark;
import com.ufpel.util.Estatisticas;
import com.ufpel.util.Monitorador;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.spark.api.java.JavaPairRDD;

/**
 *
 * @author WeslenSchiavon
 */
public class SparkWordCount {

    public static void main(String[] args) throws FileNotFoundException {
        List<Double> valoresTempo = new ArrayList<>();
        List<Double> valoresMemoria = new ArrayList<>();

        for (int i = 0; i < 10; i++) {

            Monitorador.startMemoryMonitor();
            Monitorador.startTimeMonitoring();

            System.out.println("Lendo arquivo: " + args[0]);
            Spark spark = new Spark();

            JavaPairRDD<String, Integer> ContaPalavras = spark.ContaPalavras(" ", args[0]);

            System.out.println("Coletando dados....");
            long words = ContaPalavras.count();

            valoresTempo.add(Monitorador.getTimeExecutation());
            valoresMemoria.add(Monitorador.getMaxMemoryUsage());

            System.out.println("Numero de palavras lidas: " + words);

            spark.close();
        }

        PrintStream ps = new PrintStream(new File("Relatorio.txt"));

        ps.println("Memoria:");
        valoresMemoria.forEach(valor -> ps.println(String.format(Locale.ENGLISH, "%.2f", valor)));

        ps.println();

        ps.println("Tempos:");
        valoresTempo.forEach(valor -> ps.println(String.format(Locale.ENGLISH, "%.2f", valor)));

        ps.println();
        ps.println();

        ps.println("Media Tempo: " + String.format(Locale.ENGLISH, "%.2f", Estatisticas.getMediaAritmetica(valoresTempo)) + " minutos");
        ps.println("Variancia Tempo: " + String.format(Locale.ENGLISH, "%.2f", Estatisticas.getVariancia(valoresTempo)) + " minutos");
        ps.println("Desvio padrao Tempo: " + String.format(Locale.ENGLISH, "%.2f", Estatisticas.getDesvioPadrao(valoresTempo)) + " minutos");

        ps.println();

        ps.println("Media Memoria: " + String.format(Locale.ENGLISH, "%.2f", Estatisticas.getMediaAritmetica(valoresMemoria)) + " MB");
        ps.println("Variancia Memoria: " + String.format(Locale.ENGLISH, "%.2f", Estatisticas.getVariancia(valoresMemoria)) + " MB");
        ps.println("Desvio padrao Memoria: " + String.format(Locale.ENGLISH, "%.2f", Estatisticas.getDesvioPadrao(valoresMemoria)) + " MB");

        ps.close();

    }
}
