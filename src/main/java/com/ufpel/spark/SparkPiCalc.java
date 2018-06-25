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

/**
 *
 * @author WeslenSchiavon
 */
public class SparkPiCalc {

    public static void main(String[] args) throws FileNotFoundException {
        List<Double> valoresTempo = new ArrayList<>();
        List<Double> valoresMemoria = new ArrayList<>();
        List<Double> valoresErroPi = new ArrayList<>();

        for (int i = 0; i < 10; i++) {

            System.out.println("Teste: " + i);
            Monitorador.startMemoryMonitor();
            Monitorador.startTimeMonitoring();

            Spark spark = new Spark();

            double valorPi = spark.calcPiPorTequinicaDosDardos(Integer.parseInt(args[0]));

            valoresTempo.add(Monitorador.getTimeExecutation());
            valoresMemoria.add(Monitorador.getMaxMemoryUsage());
            valoresErroPi.add(Math.abs(valorPi - Math.PI));

            System.out.println("Valor de erro encontrado: " + Math.abs(valorPi - Math.PI));

            spark.close();
        }

        PrintStream ps = new PrintStream(new File("Relatorio.txt"));

        ps.println("Memoria:");
        valoresMemoria.forEach(valor -> ps.println(String.format(Locale.ENGLISH, "%.2f", valor)));

        ps.println();

        ps.println("Tempos:");
        valoresTempo.forEach(valor -> ps.println(String.format(Locale.ENGLISH, "%.2f", valor)));

        ps.println("Erro pi:");
        valoresErroPi.forEach(valor -> ps.println(valor));

        ps.println();
        ps.println();

        ps.println("Media Tempo: " + String.format(Locale.ENGLISH, "%.2f", Estatisticas.getMediaAritmetica(valoresTempo)) + " minutos");
        ps.println("Variancia Tempo: " + String.format(Locale.ENGLISH, "%.2f", Estatisticas.getVariancia(valoresTempo)) + " minutos");
        ps.println("Desvio padrao Tempo: " + String.format(Locale.ENGLISH, "%.2f", Estatisticas.getDesvioPadrao(valoresTempo)) + " minutos");

        ps.println();

        ps.println("Media Memoria: " + String.format(Locale.ENGLISH, "%.2f", Estatisticas.getMediaAritmetica(valoresMemoria)) + " MB");
        ps.println("Variancia Memoria: " + String.format(Locale.ENGLISH, "%.2f", Estatisticas.getVariancia(valoresMemoria)) + " MB");
        ps.println("Desvio padrao Memoria: " + String.format(Locale.ENGLISH, "%.2f", Estatisticas.getDesvioPadrao(valoresMemoria)) + " MB");

        ps.println();

        ps.println("Media Erro: " + Estatisticas.getMediaAritmetica(valoresErroPi));
        ps.println("Variancia Erro: " + Estatisticas.getVariancia(valoresErroPi));
        ps.println("Desvio padrao Erro: " + Estatisticas.getDesvioPadrao(valoresErroPi));

        ps.close();

    }
}
