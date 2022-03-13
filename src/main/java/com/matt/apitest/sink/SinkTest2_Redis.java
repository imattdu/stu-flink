package com.matt.apitest.sink;

import com.matt.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author matt
 * @create 2022-01-25 23:57
 */
public class SinkTest2_Redis {

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
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("matt05")
                .setPort(6379)
                .build();
        resStream.addSink(new RedisSink<>(config, new MyRedisMapper()));


        // job name
        env.execute("my");

    }

    public static class MyRedisMapper implements RedisMapper<SensorReading> {
        // 保存到 redis 的命令，存成哈希表
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "sensor_tempe");
        }

        // key
        public String getKeyFromData(SensorReading data) {
            return data.getId();
        }

        // v
        public String getValueFromData(SensorReading data) {
            return data.getTemperatrue().toString();
        }
    }

}
