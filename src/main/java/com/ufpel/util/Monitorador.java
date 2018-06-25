/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ufpel.util;

/**
 *
 * @author WeslenSchiavon
 */
public class Monitorador {

    private static long startTime;
    private static MemoryMonitor memoryMonitor;

    public static void startTimeMonitoring() {
        Monitorador.startTime = System.currentTimeMillis();
    }

    public static double getTimeExecutation() {
        long endTime = System.currentTimeMillis();

        return (endTime - Monitorador.startTime) / 60000.0;
    }

    public static long getAtualMemoryUsage() {
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }

    public static void startMemoryMonitor() {

        Monitorador.memoryMonitor = new MemoryMonitor(1000);

        new Thread(Monitorador.memoryMonitor).start();
    }

    public static double getMaxMemoryUsage() {
        Monitorador.memoryMonitor.setStop(true);
        return Monitorador.memoryMonitor.getMaiorUsoDeMemoria() / 1000000;

    }

}
