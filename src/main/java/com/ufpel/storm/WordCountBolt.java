/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ufpel.storm;

import com.ufpel.util.SincronizaTarefas;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

//There are a variety of bolt types. In this case, use BaseBasicBolt
public class WordCountBolt extends BaseBasicBolt {

    private static AtomicLong NumeroDePalavras;
    //Create logger for this class
    private static final Logger logger = LogManager.getLogger(WordCountBolt.class);
    //For holding words and counts
    Map<String, Long> counts;
    //How often to emit a count of words
    private Integer emitFrequency;

    static {
        NumeroDePalavras = new AtomicLong(0);
    }

    public static void setNumeroDePalavras(int NumeroDePalavras) {
        WordCountBolt.NumeroDePalavras = new AtomicLong(NumeroDePalavras);
    }

    // Default constructor
    public WordCountBolt() {
        emitFrequency = 0; // Default to 60 seconds
        this.counts = new HashMap<>();
    }

    // Constructor that sets emit frequency
    public WordCountBolt(Integer frequency) {
        emitFrequency = frequency;
        this.counts = new HashMap<>();
    }

    //Configure frequency of tick tuples for this bolt
    //This delivers a 'tick' tuple on a specific interval,
    //which is used to trigger certain actions
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
        return conf;
    }

    //execute is called to process tuples
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        //If it's a tick tuple, emit all words and counts
        if (tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
            for (String word : counts.keySet()) {
                Long count = counts.get(word);
                collector.emit(new Values(word, count));
            }

            synchronized (SincronizaTarefas.LOCK) {
                //   System.out.println("Palavras:" + NumeroDePalavras.addAndGet(counts.size()));
                if (NumeroDePalavras.get() >= 10210712) { //67391491 10210712
                    SincronizaTarefas.LOCK.notify();
                }
            }

        } else {
            String word = tuple.getString(0);
            Long count = counts.get(word);
            if (count == null) {
                count = 0l;
            }
            count++;
            counts.put(word, count);
        }
    }

    //Declare that this emits a tuple containing two fields; word and count
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
