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

public class TumblingWindowMaxBolt extends BaseWindowedBolt{
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {

        float etot_max = 0;
        int tuples_etot = 0;
        float sum = 0;
        List<Tuple> tuplesInWindow = inputWindow.get();
        if (tuplesInWindow.size() > 0) {
            /*
             * Since this is a tumbling window calculation,
             * we use all the tuples in the window to compute the avg.
             */
            for (Tuple tuple : tuplesInWindow) {
                if ((float) tuple.getValue(0) > etot_max) {
                    etot_max = (float) tuple.getValue(0);
                }
                tuples_etot += 1;
               
                
            }
        
            /* for (Tuple tuple : tuplesInWindow) {
                System.out.println(tuple.getValues());
            } */
            
            System.out.println(new Values(etot_max, inputWindow.getEndTimestamp(), inputWindow.getStartTimestamp(), tuples_etot, "Etot"));
            collector.emit("avg_stream", new Values(etot_max, inputWindow.getEndTimestamp(), inputWindow.getStartTimestamp(), tuples_etot, "Etot"));
            

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("avg_stream", new Fields("avg","s_ts","e_ts","cnt"));
    }
    
}
