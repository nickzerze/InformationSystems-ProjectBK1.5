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
import java.text.SimpleDateFormat;
import org.json.JSONObject;


public class SlidingWindowMaxBolt extends BaseWindowedBolt{
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        
        float diff_etot = 0;
        float diff_wtot = 0;
        int tuples_etot = 0;
        int tuples_wtot = 0;

        List<Tuple> tuplesInWindow = inputWindow.get();

        System.out.println("Events in current window [SlidingWindowMaxBolt] : " + tuplesInWindow.size());

        if (tuplesInWindow.size() == 1) {
            System.out.println("Only one tuple in window!");
        }
        else {
            Float[] energy = {(float)0.0, (float)0.0};
            Float[] water = {(float)0.0, (float)0.0};

            
            int i=0;
            int j=0;

            if (tuplesInWindow.size() == 4) {
                for (Tuple tuple : tuplesInWindow) {
                    if (tuple.getValue(3).equals("Etot")) {
                        String[] first_mysplit =  tuple.getValue(0).toString().split("\"");
                        Float etot = Float.parseFloat(first_mysplit[11]);
                        energy[i] = etot;
                        i++;
                    }
                    else if (tuple.getValue(3).equals("Wtot")){
                        String[] first_mysplit =  tuple.getValue(0).toString().split("\"");
                        Float wtot = Float.parseFloat(first_mysplit[11]);
                        water[j] = wtot;
                        j++;
                    }
                }

                diff_etot = energy[1] - energy[0];
                diff_wtot = water[1] - water[0];

            }
            else  {
                System.out.println("Not enough tuples to calculate diff!");
            }
        }

        Date time1=new java.util.Date((long)inputWindow.getEndTimestamp());
        SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); 
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        String formattedDate = sdf.format(time1);    
        String[] splitted = formattedDate.split("\\s+");
        String es_timestamp = splitted[0]+"T"+splitted[1]+"Z";

        JSONObject json_etot = new JSONObject();
        json_etot.put("data", diff_etot);
        json_etot.put("data_raw", tuplesInWindow.toString());
        json_etot.put("cnt", tuples_etot);
        json_etot.put("@timestamp", es_timestamp);
        String str_json_etot =  json_etot.toString();

        JSONObject json_wtot = new JSONObject();
        json_wtot.put("data", diff_wtot);
        json_wtot.put("data_raw", tuplesInWindow.toString());
        json_wtot.put("cnt", tuples_wtot);
        json_wtot.put("@timestamp", es_timestamp);
        String str_json_wtot =  json_wtot.toString();

        String id = inputWindow.getEndTimestamp()+"";

        collector.emit("max_stream", new Values(str_json_etot, tuples_etot, "energy_total", "_doc", id));
        collector.emit("max_stream", new Values(str_json_wtot, tuples_wtot, "water_total", "_doc", id));


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("max_stream", new Fields("source", "cnt", "index", "type", "id"));
    }


}
