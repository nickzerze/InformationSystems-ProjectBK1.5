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

public class TumblingWindowSumBolt extends BaseWindowedBolt{
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {

        float sum_hvac = 0;
        int tuples_hvac = 0;
        float sum_miac = 0;
        int tuples_miac = 0;
        int sum_mov = 0;
        int tuples_mov = 0;
        float sum_w = 0;
        int tuples_w = 0;

        List<Tuple> tuplesInWindow = inputWindow.get();
        
        if (tuplesInWindow.size() > 0) {
            /*
             * Since this is a tumbling window calculation,
             * we use all the tuples in the window to compute the avg.
             */

            for (Tuple tuple : tuplesInWindow) {
                

                if ((tuple.getValue(3).equals("HVAC1")) || (tuple.getValue(3).equals("HVAC2"))) {
                    String[] mysplit =  tuple.getValue(0).toString().split("\"");
                    sum_hvac += Float.parseFloat(mysplit[11]);
                    tuples_hvac += 1;
                }
                else  if ((tuple.getValue(3).equals("MiAC1")) || (tuple.getValue(3).equals("MiAC2"))) {
                    String[] mysplit =  tuple.getValue(0).toString().split("\"");
                    sum_miac += Float.parseFloat(mysplit[11]);
                    tuples_miac += 1;
                }
                else if (tuple.getValue(3).equals("Mov1")) {
                    sum_mov += 1;
                    tuples_mov += 1;
                }
                else if (tuple.getValue(3).equals("W1")) {
                    String[] mysplit =  tuple.getValue(0).toString().split("\"");
                    sum_w += Float.parseFloat(mysplit[11]);
                    tuples_w += 1;
                }
                else {
                    System.out.println("Problem!");
                }
                
            }        

            Date time1=new java.util.Date((long)inputWindow.getEndTimestamp());
            SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); 
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            String formattedDate = sdf.format(time1);    
            String[] splitted = formattedDate.split("\\s+");
            String es_timestamp = splitted[0]+"T"+splitted[1]+"Z";

            JSONObject json_hvac = new JSONObject();
            json_hvac.put("data", sum_hvac);
            json_hvac.put("raw_data", tuplesInWindow.toString());
            json_hvac.put("cnt", tuples_hvac);
            json_hvac.put("@timestamp", es_timestamp);
            String str_json_hvac =  json_hvac.toString();


            JSONObject json_miac = new JSONObject();
            json_miac.put("data", sum_miac);
            json_miac.put("raw_data", tuplesInWindow.toString());
            json_miac.put("cnt", tuples_miac);
            json_miac.put("@timestamp", es_timestamp);
            String str_json_miac =  json_miac.toString();

            JSONObject json_mov = new JSONObject();
            json_mov.put("data", sum_mov);
            json_mov.put("raw_data", tuplesInWindow.toString());
            json_mov.put("cnt", tuples_mov);
            json_mov.put("@timestamp", es_timestamp);
            String str_json_mov =  json_mov.toString();

            JSONObject json_w = new JSONObject();
            json_w.put("data", sum_w);
            json_w.put("raw_data", tuplesInWindow.toString());
            json_w.put("cnt", tuples_w);
            json_w.put("@timestamp", es_timestamp);
            String str_json_w =  json_w.toString();

            String id = inputWindow.getEndTimestamp()+"";

            collector.emit("sum_stream", new Values(str_json_hvac, tuples_hvac, "hvac", "_doc", id));
            collector.emit("sum_stream", new Values(str_json_miac, tuples_miac, "miac", "_doc", id));
            collector.emit("sum_stream", new Values(str_json_mov, tuples_mov, "mov", "_doc", id));
            collector.emit("sum_stream", new Values(str_json_w, tuples_w, "water_day", "_doc", id));

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("sum_stream", new Fields("source", "cnt", "index", "type", "id"));
        //declarer.declareStream("late_stream", new Fields("source", "cnt", "index", "type", "id"));
    }



}
