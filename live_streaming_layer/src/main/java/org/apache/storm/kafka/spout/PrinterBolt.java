package org.apache.storm.kafka.spout;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import java.util.Map;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.json.JSONObject;



public class PrinterBolt extends BaseBasicBolt {
    private OutputCollector collector;

    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println("Late tuple: "+tuple.getValues().toString());
        
        int startingIndex = tuple.getValues().toString().indexOf("{");
        int closingIndex = tuple.getValues().toString().indexOf("}");
        startingIndex = tuple.getValues().toString().indexOf("{", closingIndex + 1);
        closingIndex = tuple.getValues().toString().indexOf("}", closingIndex + 1);
        String result2 = tuple.getValues().toString().substring(startingIndex + 1, closingIndex);

        

        String[] splitted = result2.split("\"");
        JSONObject json1 = new JSONObject();       
        json1.put("@timestamp", splitted[3]);
        json1.put("data", splitted[11]);
        String temp1 =  json1.toString();

        String[] splitted2 = tuple.getValues().toString().split(",");
        String id = splitted2[6].substring(1);

        /* String[] mysplit =  tuple.getValue(0).toString().split("\"");
        Float value = Float.parseFloat(mysplit[11]);*/

        collector.emit("late_stream", new Values(temp1, "late", "_doc", id)); 
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("late_stream", new Fields("source", "index", "type", "id"));
    }

}