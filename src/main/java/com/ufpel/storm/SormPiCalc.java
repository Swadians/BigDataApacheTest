/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ufpel.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 *
 * @author WeslenSchiavon
 */
public class SormPiCalc {

    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomNumberSpout(100000000), 1);

        builder.setBolt("pi", new CalcPiBolt(), 3).shuffleGrouping("spout");

        Config conf = new Config();
        conf.setDebug(false);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("pi-calc", conf, builder.createTopology());

        //sleep
        Thread.sleep(Long.MAX_VALUE);
        //shut down the cluster
        cluster.shutdown();
    }
}
