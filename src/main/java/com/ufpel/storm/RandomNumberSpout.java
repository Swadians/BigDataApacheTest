/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ufpel.storm;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 *
 * @author WeslenSchiavon
 */
public class RandomNumberSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private Random rand;
    private static AtomicLong numberGenereted;
    private final long numIteracoes;

    static {
        RandomNumberSpout.numberGenereted = new AtomicLong(0);
    }

    public RandomNumberSpout(long numIteracoes) {
        this.numIteracoes = numIteracoes;

        System.out.println("Iniciando produtor");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("numberX", "numberY", "iteracao"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.rand = new Random();
    }

    @Override
    public void nextTuple() {
        long id = this.numberGenereted.incrementAndGet();

        if (id == this.numIteracoes) {
            synchronized (this) {
                this.collector.emit(new Values(-1.0, -1.0, id - 1.0), id);
            }
        } else {
            this.collector.emit(new Values(Math.random(), Math.random(), id), id);
        }

    }

    @Override
    public void fail(Object id) {
        System.err.println("Failed  " + id);
    }
}
