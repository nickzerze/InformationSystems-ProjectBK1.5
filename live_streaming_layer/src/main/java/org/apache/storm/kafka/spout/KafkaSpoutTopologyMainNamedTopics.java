/*
* Licensed to the Apache Software Foundation (ASF) under one
*   or more contributor license agreements.  See the NOTICE file
*   distributed with this work for additional information
*   regarding copyright ownership.  The ASF licenses this file
*   to you under the Apache License, Version 2.0 (the
*   "License"); you may not use this file except in compliance
*   with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
*   Unless required by applicable law or agreed to in writing, software
*   distributed under the License is distributed on an "AS IS" BASIS,
*   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*   See the License for the specific language governing permissions and
*   limitations under the License.
*/

package org.apache.storm.kafka.spout;

import java.util.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.commons.lang3.StringUtils;
import java.util.Date;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.elasticsearch.common.DefaultEsTupleMapper;
import org.apache.storm.elasticsearch.bolt.EsIndexBolt;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.json.JSONObject;

/**
 * This example sets up 3 topologies to put data in Kafka via the KafkaBolt,
* and shows how to set up a topology that reads from some Kafka topics using the KafkaSpout.
*/
public class KafkaSpoutTopologyMainNamedTopics {

    private static final String KAFKA_LOCAL_BROKER = "localhost:9092";
    public static final String TOPIC_AV = "average";

    public static void main(String[] args) throws Exception {
        new KafkaSpoutTopologyMainNamedTopics().runMain(args);       
    }

    protected void runMain(String[] args) throws Exception {
        final String brokerUrl = KAFKA_LOCAL_BROKER;
        System.out.println("Running with broker url: " + brokerUrl);
        
        //KafkaSpout<String, String> kafkaspout = new KafkaSpout<String, String>(spoutConfig);
        
        //Consumer. Sets up a topology that reads the given Kafka spouts and logs the received messages
        Config conf = new Config();
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_DEBUG, false);

        //builder.createTopology();  
        StormSubmitter.submitTopology("storm-kafka-client-spout-test", conf, getTopologyKafkaSpout(getKafkaSpoutConfig(brokerUrl)));
        while (true) {
            
        }

    }

    protected StormTopology getTopologyKafkaSpout(KafkaSpoutConfig<String, String> spoutConfig) {
        TopologyBuilder builder = new TopologyBuilder();   

        KafkaSpout<String, String> kafkaspout_avg = new KafkaSpout<String, String>(spoutConfig);     


        builder.setSpout("kafka-spout", kafkaspout_avg, 1);
        builder.setBolt("tumblingavg", new TumblingWindowAvgBolt()
                        .withTumblingWindow(new Duration(1, TimeUnit.DAYS))
                        .withTimestampField("ts"), 1)
                        .shuffleGrouping("kafka-spout", "avg_stream");
        builder.setBolt("tumblingsum", new TumblingWindowSumBolt()
                        .withTumblingWindow(new Duration(1, TimeUnit.DAYS))
                        .withTimestampField("ts")
                        .withLateTupleStream("late_stream")
                        .withLag(new Duration(2, TimeUnit.DAYS)), 1
                ).shuffleGrouping("kafka-spout", "sum_stream");
        builder.setBolt("slidingmax", new SlidingWindowMaxBolt()
                        .withWindow(new Duration(2, TimeUnit.DAYS), new Duration(1, TimeUnit.DAYS))
                        .withTimestampField("ts"), 1)
                        .shuffleGrouping("kafka-spout", "max_stream"); 
        builder.setBolt("processingbolt",new PrinterBolt(), 1)
                        .shuffleGrouping("tumblingsum", "late_stream");

        EsConfig esConfig = new EsConfig("http://localhost:9200");

        Header[] headers = new Header[1];
        headers[0]= new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        esConfig.withDefaultHeaders(headers);

        EsTupleMapper tupleMapper = new DefaultEsTupleMapper();
        builder.setBolt("myesbolt", new EsIndexBolt(esConfig, tupleMapper), 1)
               .shuffleGrouping("tumblingavg", "avg_stream")
               .shuffleGrouping("slidingmax", "max_stream")
               .shuffleGrouping("tumblingsum", "sum_stream")
               .shuffleGrouping("kafka-spout", "sum_stream")
               .shuffleGrouping("kafka-spout", "avg_stream")
               .shuffleGrouping("kafka-spout", "max_stream")
               .shuffleGrouping("processingbolt", "late_stream");
        
        return builder.createTopology();
    }

    public static class OnlyValueRecordTranslator<K, V> implements RecordTranslator<K, V> {

	    private static final long serialVersionUID = 1L;

		@Override
        public List<Object> apply(ConsumerRecord<K, V> record) {
            
            //System.out.println(record.value().toString());
            String[] val = record.value().toString().split(",");
            String[] splitted = val[0].split("\\s+");
            
            String key = record.key().toString();
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
            Date date;

            String type = splitted[0].substring(1);

            try {
                date = format.parse(splitted[1]+" "+splitted[2]);
                long timestamp = date.getTime(); 

                
                SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); 
                sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                String formattedDate = sdf.format(date);    
                String[] split_date = formattedDate.split("\\s+");
                String es_timestamp = split_date[0]+"T"+split_date[1]+"Z";

                JSONObject json1 = new JSONObject();
                json1.put("value", StringUtils.chop(splitted[4]));
                json1.put("@timestamp", es_timestamp);
                json1.put("type", type);
                String str_val =  json1.toString();

                //return new Values(Float.parseFloat(val[0]), System.currentTimeMillis() - (24 * 60 * 60 * 1000), key );

                if (type.equals("TH1")  || type.equals("TH2")) {
                    return new KafkaTuple(str_val, timestamp, key, type, "_doc", "raw_data").routedTo("avg_stream");
                }
                else if (type.equals("HVAC1")  || type.equals("HVAC2")) {
                    //System.out.println("FLAG2");
                    return new KafkaTuple(str_val, timestamp, key, type, "_doc", "raw_data").routedTo("sum_stream");
                }
                else if (type.equals("MiAC1")  || type.equals("MiAC2")) {
                    //System.out.println("FLAG2");
                    return new KafkaTuple(str_val, timestamp, key, type, "_doc", "raw_data").routedTo("sum_stream");
                }
                else if (type.equals("Etot")) {
                    //System.out.println("FLAG3");
                    return new KafkaTuple(str_val, timestamp, key, type, "_doc", "raw_data").routedTo("max_stream");
                }
                else if (type.equals("Mov1")) {
                    //System.out.println("FLAG3");
                    return new KafkaTuple(str_val, timestamp, key, type, "_doc", "raw_data").routedTo("sum_stream");
                }
                else if (type.equals("W1")) {
                    //System.out.println("FLAG3");
                    return new KafkaTuple(str_val, timestamp, key, type, "_doc", "raw_data").routedTo("sum_stream");
                }
                else if (type.equals("Wtot")) {
                    //System.out.println("FLAG3");
                    return new KafkaTuple(str_val, timestamp, key, type, "_doc", "raw_data").routedTo("max_stream");
                }
                else {
                    System.out.println("No Type Specified in Translator!");
                    return new KafkaTuple();
                }

                
            } catch (ParseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return new Values(val[0], System.currentTimeMillis() - (24 * 60 * 60 * 1000), key, type);
            }
                  
        }

        @Override
        public Fields getFieldsFor(String stream) {
            return new Fields("source", "ts", "id", "category", "type", "index");
        }

        @Override
        public List<String> streams(){
            List<String> streams_list = Arrays.asList("max_stream", "avg_stream", "sum_stream", "default", "raw_data", "late_stream");
            return streams_list;
        }
 

    }  

    protected static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(TimeInterval.microSeconds(500),
            TimeInterval.milliSeconds(2), Integer.MAX_VALUE, TimeInterval.seconds(10));
    }

    static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers) {

        return KafkaSpoutConfig.builder(bootstrapServers, new String[]{"average", "sum", "max"})
            .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.EARLIEST)
            .setProp(ConsumerConfig.GROUP_ID_CONFIG,"average")
            .setRecordTranslator(new OnlyValueRecordTranslator<String, String>())
            .build();
    }

}