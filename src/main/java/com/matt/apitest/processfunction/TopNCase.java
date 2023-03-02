package com.matt.apitest.processfunction;

import com.matt.apitest.beans.Event;
import com.matt.apitest.model.UrlCntBO;
import com.matt.apitest.source.SourceTest4_UDF;
import com.matt.apitest.window.UrlCntCase;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

/**
 * @author matt
 * @create 2023-03-01 00:35
 * @desc 访问最多的2个用户
 */
public class TopNCase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new SourceTest4_UDF.ParallelCustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        SingleOutputStreamOperator<UrlCntBO> urlCntStream = stream.keyBy(d -> d.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlCntCase.UrlCntAgg(), new UrlCntCase.UrlCntResult());

        urlCntStream.keyBy(d -> d.winEnd)
                .process(new TopNProcessResult(2))
                .print();


        // 方式一： 没有分组
       /* stream.map(d -> d.user)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10L), Time.seconds(5L)))
                .aggregate(new UrlHashMapCountAgg(), new UrlAllWinResult())
                .print();*/

        env.execute();
    }

    public static class UrlHashMapCountAgg implements AggregateFunction<String, HashMap<String, Long>, ArrayList<Tuple2<String, Long>>> {

        @Override
        public HashMap<String, Long> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public HashMap<String, Long> add(String s, HashMap<String, Long> acc) {
            if (!acc.containsKey(s)) {
                acc.put(s, 0L);
            }
            acc.put(s, acc.get(s) + 1);
            return acc;
        }

        @Override
        public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> acc) {
            ArrayList<Tuple2<String, Long>> tList = new ArrayList<>();
            for (Map.Entry<String, Long> entry : acc.entrySet()) {
                tList.add(Tuple2.of(entry.getKey(), entry.getValue()));
            }
            tList.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    long diff = o2.f1 - o1.f1;
                    if (diff > 0) {
                        return 1;
                    } else if (diff == 0) {
                        return 0;
                    } else {
                        return -1;
                    }
                }
            });
            return tList;
        }

        @Override
        public HashMap<String, Long> merge(HashMap<String, Long> stringLongHashMap, HashMap<String, Long> acc1) {
            return null;
        }
    }


    public static class UrlAllWinResult extends ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow> {
        @Override
        public void process(ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow>.Context context, Iterable<ArrayList<Tuple2<String, Long>>> iterable, Collector<String> collector) throws Exception {
            ArrayList<Tuple2<String, Long>> tList = iterable.iterator().next();
            StringBuilder res = new StringBuilder();
            for (int i = 0; i < 2; i++) {
                res.append(tList.get(i));
            }
            collector.collect(String.valueOf(res));
        }
    }


    public static class TopNProcessResult extends KeyedProcessFunction<Long, UrlCntBO, String> {

        public int n;

        // 状态
        private ListState<UrlCntBO> urlCntBOListState;

        public TopNProcessResult(int n) {
            this.n = n;
        }

        // 环境中获取状态
        @Override
        public void open(Configuration parameters) throws Exception {
            urlCntBOListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlCntBO>("url-cnt-list", Types.POJO(UrlCntBO.class))
            );
        }

        @Override
        public void processElement(UrlCntBO v, KeyedProcessFunction<Long, UrlCntBO, String>.Context context, Collector<String> collector) throws Exception {
            urlCntBOListState.add(v);
            // 定时器
            context.timerService().registerProcessingTimeTimer(context.getCurrentKey() + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlCntBO, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            List<Tuple2<String, Long>> tList = new ArrayList<>();
            for (UrlCntBO u : urlCntBOListState.get()) {
                tList.add(Tuple2.of(u.url, u.cnt));
            }
            tList.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    long diff = o2.f1 - o1.f1;
                    if (diff > 0) {
                        return 1;
                    } else if (diff == 0) {
                        return 0;
                    } else {
                        return -1;
                    }
                }
            });
            String sb = "";
            if (tList.size() >= 2) {
                sb = tList.get(0).f0 + tList.get(0).f1 +
                        tList.get(1).f0 + tList.get(1).f1;
            }

            out.collect(String.valueOf(sb));
        }
    }

}
