package com.matt.apitest.sink;

import com.matt.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author matt
 * @create 2022-01-26 0:21
 */
public class SinkTest3_ES {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        DataStream<String> dataStream = env.readTextFile("/Users/matt/workspace/java/bigdata/study-flink/src/main/resources/sensor.txt");

        dataStream.print("file");

        DataStream<SensorReading> resStream = dataStream.map(line -> {
            String[] f = line.split(",");
            return new SensorReading(f[0], new Long(f[1]), new Double(f[2]));
        });

        resStream.print();
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200));

        resStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts, new MyEsSinkFunction()).build());

        // job name
        env.execute("my");

    }

    public static class MyEsSinkFunction implements
            ElasticsearchSinkFunction<SensorReading> {
        @Override
        public void process(SensorReading element, RuntimeContext ctx, RequestIndexer
                indexer) {
            HashMap<String, String> dataSource = new HashMap<>();
            dataSource.put("id", element.getId());
            dataSource.put("ts", String.valueOf(element.getTimestamp()));
            dataSource.put("temp", element.getTemperatrue().toString());
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor")
                    .type("readingData")
                    .source(dataSource);
            indexer.add(indexRequest);
        }
    }

}
