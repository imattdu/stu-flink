package com.matt.apitest.source;

import com.matt.apitest.beans.Event;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * 自定义数据源
 *
 * @author matt
 * @create 2022-01-16 15:50
 */
public class SourceTest4_UDF {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);

        DataStream<Event> dataStream = env.addSource(new ParallelCustomSource()).setParallelism(2);

        dataStream.print("kafka");

        // job name
        env.execute("cu");


    }

    // 并行度是1
    public static class MySensorSource implements SourceFunction<Event> {
        // 标志位 控制数据的产生
        private boolean running = true;

        /**
         * 功能：
         *
         * @param ctx 1
         * @author matt
         * @date 2022/1/16
         */
        @Override
        public void run(SourceContext<Event> ctx) throws InterruptedException {
            // 随机数发生器
            Random random = new Random();

            HashMap<String, Long> sensorTempMap = new HashMap<>();
            for (int i = 0; i < 5; i++) {
                sensorTempMap.put("sensor_" + (i + 1), System.currentTimeMillis());
            }

            while (running) {
                for (String sensorId : sensorTempMap.keySet()) {
                    // 当前温度随机波动
                    Long ts = sensorTempMap.get(sensorId) + new Random().nextInt(1000000);
                    sensorTempMap.put(sensorId, ts);
                    ctx.collect(new Event("matt", "matt", ts));
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


    public static class ParallelCustomSource implements ParallelSourceFunction<Event> {
        // 标志位 控制数据的产生
        private boolean running = true;

        /**
         * 功能：
         *
         * @param ctx 1
         * @author matt
         * @date 2022/1/16
         */
        @Override
        public void run(SourceContext<Event> ctx) throws InterruptedException {
            // 随机数发生器
            Random random = new Random();
            List<String> userList = Arrays.asList("matt", "jack", "lisi", "lb", "df");
            List<String> urlList = Arrays.asList("/save", "/remove", "/update", "/list", "/detail");

            while (running) {

                for (int i = 0; i < 5; i++) {
                    int randV = random.nextInt(5);
                    ctx.collect(new Event(userList.get(randV), urlList.get(randV), System.currentTimeMillis()));
                    Thread.sleep(1000L);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
