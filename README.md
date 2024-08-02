# InformationSystems-ProjectBK1.5
ECE NTUA - Information Systems, Analysis and Design Class Project (2022-23, Fall Semester)

ΒK 1.5) Real-time data stream processing using the open source distributed systems  Apache Kafka, Apache Storm, ElasticSearch and Kibana+websockets.

In this particular project we are going to use combined open source tools for the creation of a live streaming system that will be the prototype of a real IoT system. In particular, a system will be created in which the sensors will send virtual data to real time to the Kafka Message broker and then the data will be converted to daily or other conversions using Apache Storm and will be stored in a NoSQL Database, ElasticSearch. Finally, the data will be presented in Dashboards using of the open source tool Kibana and using websockets will be presented live.

| *Layer* | *Tool*    |
| :---:   | :---: | 
| Messaging Systems-Broker | Apache Kafka [1]   | 
| Live Streaming Processing System  | Apache Storm [2]   |
| Database   | ElasticSearch [3]   |
| Presentation Layer   | Kibana [4]   |

[1] https://kafka.apache.org/
[2] http://storm.apache.org/index.html
[3] https://www.elastic.co/elasticsearch/
[4] https://www.elastic.co/kibana/