package org.apache.storm.kafka.spout;

import java.util.*;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import java.util.Map;
import org.json.JSONObject;
import java.text.SimpleDateFormat;

public class TumblingWindowAvgBolt extends BaseWindowedBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {

        
        float sum = 0;
        List<Tuple> tuplesInWindow = inputWindow.get();

        System.out.println("Events in current window [TumblingWindowAvgBolt]: " + tuplesInWindow.size());

        if (tuplesInWindow.size() > 0) {
            /*
             * Since this is a tumbling window calculation,
             * we use all the tuples in the window to compute the avg.
             */
            for (Tuple tuple : tuplesInWindow) {

                if (tuple.getValue(3).equals("TH1") || tuple.getValue(3).equals("TH2")) {
                    String[] mysplit =  tuple.getValue(0).toString().split("\"");
                    sum += Float.parseFloat(mysplit[11]);
                }
                
            }

            float avg = sum/tuplesInWindow.size();

            Date time1=new java.util.Date((long)inputWindow.getEndTimestamp());
            SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); 
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            String formattedDate = sdf.format(time1);    
            String[] splitted = formattedDate.split("\\s+");
            String es_timestamp = splitted[0]+"T"+splitted[1]+"Z";

            JSONObject json1 = new JSONObject();
            json1.put("data", avg);
            json1.put("raw_data", tuplesInWindow.toString());
            json1.put("cnt", tuplesInWindow.size());
            json1.put("@timestamp", es_timestamp);
            String temp1 =  json1.toString();

            String id = inputWindow.getEndTimestamp()+"";

            collector.emit("avg_stream", new Values(temp1, tuplesInWindow.size(), "temperature", "_doc", id));
            
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("avg_stream", new Fields("source", "cnt", "index", "type", "id"));
    }
}