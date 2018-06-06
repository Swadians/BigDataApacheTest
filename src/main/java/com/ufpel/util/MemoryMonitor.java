/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ufpel.util;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author WeslenSchiavon
 */
public class MemoryMonitor implements Runnable {

    private int intervalTime;
    private long maiorUsoDeMemoria;
    private boolean stop;

    public MemoryMonitor(int intervalTime) {
        this.intervalTime = intervalTime;
    }

    public long getMaiorUsoDeMemoria() {
        return maiorUsoDeMemoria;
    }

    public void setStop(boolean stop) {
        this.stop = stop;
    }

    @Override
    public void run() {
        while (!this.stop) {

            synchronized (this) {
                long memoriaAtual = Monitorador.getAtualMemoryUsage();
                if (memoriaAtual > this.maiorUsoDeMemoria) {
                    this.maiorUsoDeMemoria = Monitorador.getAtualMemoryUsage();
                }
            }
            try {
                Thread.sleep(intervalTime);
            } catch (InterruptedException ex) {
                Logger.getLogger(MemoryMonitor.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
