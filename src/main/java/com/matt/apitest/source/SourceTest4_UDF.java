package com.matt.apitest.source;

import com.matt.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

/**
 * @author matt
 * @create 2022-01-16 15:50
 */
public class SourceTest4_UDF {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStream<SensorReading> dataStream = env.addSource(new MySensorSouce());

        dataStream.print("kafka");

        // job name
        env.execute("kafak_job");


    }

    public static class MySensorSouce implements SourceFunction<SensorReading> {
        // 标志位 控制数据的产生
        private boolean running = true;

        /**
         * 功能：
         *
         * @param ctx
         * @return void
         * @author matt
         * @date 2022/1/16
         */
        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            // 随机数发生器
            Random random = new Random();

            HashMap<String, Double> sensorTempMap = new HashMap();
            for (int i = 0; i < 10; i++) {
                sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
            }

            while (running) {
                for (String sensorId : sensorTempMap.keySet()) {
                    // 当前温度随机波动
                    Double newtemp = sensorTempMap.get(sensorId) + random.nextDouble();
                    sensorTempMap.put(sensorId, newtemp);
                    ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), newtemp));
                }

                // 控制输出评率
                Thread.sleep(5000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
