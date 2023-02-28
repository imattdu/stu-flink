package com.matt.apitest.source;

import com.matt.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author matt
 * @create 2022-01-16 14:54
 */
public class SourceTest2_File {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);
        // /Users/matt/workspace/java/stu/stu-flink/src/main/resources
        DataStream<String> dataStream = env.readTextFile("/Users/matt/workspace/java/stu/stu-flink/src/main/resources/sensor.txt");

        dataStream.print("file");

        // job name
        env.execute("f_source");

    }

}
