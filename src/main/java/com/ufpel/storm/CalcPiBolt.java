/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ufpel.storm;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 *
 * @author WeslenSchiavon
 */
public class CalcPiBolt extends BaseBasicBolt {

    private static AtomicLong count;

    static {
        CalcPiBolt.count = new AtomicLong(0);
    }

    public CalcPiBolt() {
        System.out.println("Iniciando consumidor");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("result"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Double x = input.getDouble(0);
        Double y = input.getDouble(1);
        if (x == -1 && y == -1) {
            double result = 4.0 * CalcPiBolt.count.get() / input.getDouble(2);

            collector.emit(new Values(result));
            System.out.println("Pi: " + result);

            synchronized (this) {
                try {
                    this.wait();
                } catch (InterruptedException ex) {
                    System.out.println("Nao deu pra parar");
                }
            }

        } else {
            double somaDosQuadrados = x * x + y * y;

            if (somaDosQuadrados < 1) {
                CalcPiBolt.count.incrementAndGet();
            }
        }

    }

}
