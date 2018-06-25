/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ufpel.util;

import java.util.List;

/**
 *
 * @author WeslenSchiavon
 */
public class Estatisticas {

    public static double getSomaDosElementosAoQuadrado(List<Double> valores) {

        double total = 0;

        for (int counter = 0; counter < valores.size(); counter++) {
            total += Math.pow(valores.get(counter), 2);
        }

        return total;

    }

    public static double getSomaDosElementos(List<Double> valores) {

        double total = 0;

        for (int counter = 0; counter < valores.size(); counter++) {
            total += valores.get(counter);
        }

        return total;

    }

    public static double getMediaAritmetica(List<Double> valores) {

        double total = 0;

        for (int counter = 0; counter < valores.size(); counter++) {
            total += valores.get(counter);
        }

        return total / valores.size();

    }

    public static double getVariancia(List<Double> valores) {

        double p1 = 1 / Double.valueOf(valores.size() - 1);

        double p2 = getSomaDosElementosAoQuadrado(valores)
                - (Math.pow(getSomaDosElementos(valores), 2) / Double
                .valueOf(valores.size()));

        return p1 * p2;

    }

    public static double getDesvioPadrao(List<Double> valores) {

        return Math.sqrt(getVariancia(valores));

    }

}
