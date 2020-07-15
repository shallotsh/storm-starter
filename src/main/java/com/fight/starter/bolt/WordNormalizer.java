package com.fight.starter.bolt;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class WordNormalizer extends BaseRichBolt {

    private OutputCollector collector;

    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String str = input.getStringByField("sentence");
        String[] values = str.split(" ");
        for(String word : values){
            if(StringUtils.isBlank(word)){
                continue;
            }
            this.collector.emit(new Values(word.trim(), 1));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "num"));
    }
}
