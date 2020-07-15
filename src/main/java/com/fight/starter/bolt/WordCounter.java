package com.fight.starter.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class WordCounter extends BaseRichBolt {

    private Integer id;
    private String name;
    private Map<String, String> counters;
    private Jedis jedis;

    @Override
    public void cleanup() {
        super.cleanup();
        System.out.println("-- Word Counter ["+name+"-"+id+"] --");
        for(Map.Entry<String, String> entry : counters.entrySet()){
            System.out.println(entry.getKey()+": "+entry.getValue());
        }
    }

    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.id = context.getThisTaskId();
        this.name = context.getThisComponentId();
        this.counters = new HashMap<String, String>();
        jedis = new Jedis("redis", 6379);
    }

    public void execute(Tuple input) {
        String word = (String) input.getValueByField("word");
        Integer num = (Integer) input.getValueByField("num");

        //1查看单词对应的value是否存在
        Integer integer = counters.get(word)==null?0:Integer.parseInt(counters.get(word)) ;
        if (integer == null || integer.intValue() == 0) {
            counters.put(word, num+"");
        } else {
            counters.put(word, (integer.intValue() + num)+"");
        }
        //2.打印数据
        System.out.println(counters);
        //保存数据到redis
        //redis key wordcount:Map
        jedis.hmset("wordcount",counters);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
