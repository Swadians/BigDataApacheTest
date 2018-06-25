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
public class SincronizaTarefas {

    public static final SincronizaTarefas LOCK;
    public static final SincronizaTarefas LOCK_BOLTS;

    static {
        LOCK = new SincronizaTarefas();
        LOCK_BOLTS = new SincronizaTarefas();
    }

}
