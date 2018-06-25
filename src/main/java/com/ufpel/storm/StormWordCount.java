/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ufpel.storm;

import com.ufpel.util.Estatisticas;
import com.ufpel.util.Monitorador;
import com.ufpel.util.SincronizaTarefas;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 *
 * @author WeslenSchiavon
 */
public class StormWordCount {

    public static void main(String[] args) {
        List<Double> valoresTempo = new ArrayList<>();
        List<Double> valoresMemoria = new ArrayList<>();
        LocalCluster cluster = null;

        for (int i = 0; i < 1; i++) {
            System.out.println("Test: " + i);
            synchronized (SincronizaTarefas.LOCK) {
                WordCountBolt.setNumeroDePalavras(0);
                WordCountBolt wordCount = new WordCountBolt();

                TopologyBuilder builder = new TopologyBuilder();

                builder.setSpout("spout", new SentenceSpout(args[0]), 1);

                builder.setBolt("split", new SplitSentenceSpout(), 8).shuffleGrouping("spout");

                builder.setBolt("count", wordCount, 12).fieldsGrouping("split", new Fields("word"));

                Config conf = new Config();

                conf.setDebug(true);

                conf.setMaxTaskParallelism(3);

                cluster = new LocalCluster();

                Monitorador.startMemoryMonitor();
                Monitorador.startTimeMonitoring();

                cluster.submitTopology("word-count", conf, builder.createTopology());

                try {
                    SincronizaTarefas.LOCK.wait();
                } catch (InterruptedException ex) {
                    Logger.getLogger(StormWordCount.class.getName()).log(Level.SEVERE, null, ex);
                }

                valoresTempo.add(Monitorador.getTimeExecutation());
                valoresMemoria.add(Monitorador.getMaxMemoryUsage());
                System.out.println("Finalizando...");
            }
            try {
                cluster.killTopology("word-count");
            } catch (Exception e) {
                System.out.println("Deu Ruim");
            }

            System.out.println("Finalizado!");

        }

        System.out.println("Salvando Relatorio...");
        try (PrintStream ps = new PrintStream(new File("Relatorio.txt"))) {
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
        } catch (FileNotFoundException ex) {
            Logger.getLogger(StormWordCount.class.getName()).log(Level.SEVERE, null, ex);
        }

        System.exit(0);
    }
//    }
}
