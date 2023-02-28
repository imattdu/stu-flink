package com.matt.apitest.sink;

import com.matt.apitest.beans.Event;
import com.matt.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author matt
 * @create 2022-01-25 23:57
 */
public class Sink2Redis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> dataStream = env.fromElements(
                new Event("a1", "1", 1L),
                new Event("a2", "2", 1L),
                new Event("a3", "3", 1L),
                new Event("a4", "4", 1L));

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("127.0.0.1")
                .setPort(6379)
                .build();
        dataStream.addSink(new RedisSink<>(config, new MyRedisMapper()));

        env.execute("redis");
    }

    public static class MyRedisMapper implements RedisMapper<Event> {
        // 保存到 redis 的命令，存成哈希表
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "test");
        }
        // key
        public String getKeyFromData(Event data) {
            return data.user;
        }
        // v
        public String getValueFromData(Event data) {
            return data.url;
        }
    }

}
