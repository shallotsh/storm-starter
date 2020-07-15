package com.fight.starter;

import com.fight.starter.bolt.WordCounter;
import com.fight.starter.bolt.WordNormalizer;
import com.fight.starter.spout.WordReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class StartApp {
    public static void main(String[] args) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader",new WordReader());
        builder.setBolt("word-normalizer", new WordNormalizer())
                .shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(),1)
                .fieldsGrouping("word-normalizer", new Fields("word"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Topology", new Config(), builder.createTopology());
//		StormSubmitter.submitTopology("Word-COUNT-Topology", conf, builder.createTopology());
    }
}
