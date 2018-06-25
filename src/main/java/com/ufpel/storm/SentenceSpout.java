/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ufpel.storm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
public class SentenceSpout extends BaseRichSpout {

    private AtomicLong linesRead;
    private BufferedReader reader;
    private String fileName;
    SpoutOutputCollector _collector;
    Random _rand;

    public SentenceSpout(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();

        linesRead = new AtomicLong(0);
        _collector = collector;
        try {
            reader = new BufferedReader(new FileReader(fileName));
            // read and ignore the header if one exists
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void nextTuple() {
        try {
            String line = reader.readLine();
            if (line != null) {
                long id = linesRead.incrementAndGet();
                _collector.emit(new Values(line), id);
            } else {
                System.out.println("Finished reading file, " + linesRead.get() + " lines read");

                synchronized (this) {
                    this.wait();
                }

            }
        } catch (IOException | InterruptedException e) {
            System.out.println("Erro na leitura!");
        }
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void deactivate() {
        try {
            reader.close();
        } catch (IOException e) {
        }
    }

}
