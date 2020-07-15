package com.fight.starter.spout;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class WordReader extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private TopologyContext context;
    private BufferedReader bufferedReader;

    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.context = context;
        try {
            bufferedReader = new BufferedReader(new FileReader(conf.get("wordsFile").toString()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void nextTuple() {
        try {
            String line = bufferedReader.readLine();
            if(StringUtils.isNotBlank(line)){
                this.collector.emit(new Values(line), UUID.randomUUID());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}
