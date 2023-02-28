package com.matt.apitest.sink;

import com.matt.apitest.beans.Event;
import com.matt.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

/**
 * @author matt
 * @create 2022-01-26 0:21
 */
public class Sink2ES {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> dataStream = env.fromElements(
                new Event("a1", "1", 1L),
                new Event("a2", "2", 1L),
                new Event("a3", "3", 1L),
                new Event("a4", "4", 1L));

        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200));
        dataStream.addSink(new ElasticsearchSink.Builder<Event>( httpHosts,new MyEsSinkFunction()).build());

        // job name
        env.execute("sink_es");

    }

    public static class MyEsSinkFunction implements
            ElasticsearchSinkFunction<Event> {
        @Override
        public void process(Event element, RuntimeContext ctx, RequestIndexer
                indexer) {
            HashMap<String, String> dataSource = new HashMap<>();
            dataSource.put("user", element.user);
            dataSource.put("url", element.url);
            dataSource.put("ts", String.valueOf(element.timestamp));
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("tt4")
                    .type("tt")
                    .source(dataSource);
            indexer.add(indexRequest);
        }
    }

}
