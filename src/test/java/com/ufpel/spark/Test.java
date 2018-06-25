/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ufpel.spark;

import com.ufpel.util.Estatisticas;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author WeslenSchiavon
 */
public class Test {

    public static void main(String[] args) {
        List<Double> valoresMemoria = new ArrayList<>();

        valoresMemoria.add(0.0001717033);
        valoresMemoria.add(0.0000268553);
        valoresMemoria.add(0.00004616869);
        valoresMemoria.add(0.00007952069);
        valoresMemoria.add(0.0001080793);
        valoresMemoria.add(0.0001094153);
        valoresMemoria.add(0.0000567513);
        valoresMemoria.add(0.00002188069);
        valoresMemoria.add(0.0000954953);
        valoresMemoria.add(0.0000693513);

        System.out.println(Estatisticas.getMediaAritmetica(valoresMemoria));

    }
}
