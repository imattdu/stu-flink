package com.matt.apitest.sink;

import com.matt.apitest.beans.Event;
import com.matt.apitest.beans.SensorReading;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @author matt
 * @create 2022-01-26 1:41
 */
public class Sink2MySQL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> dataStream = env.fromElements(
                new Event("a1", "1", 1L),
                new Event("a2", "2", 1L),
                new Event("a3", "3", 1L),
                new Event("a4", "4", 1L));


        String sql = "INSERT INTO `event` (user, url) VALUES(?, ?)";
        dataStream.addSink(JdbcSink.sink(sql, ((statement, event) -> {
                    statement.setString(1, event.user);
                    statement.setString(2, event.url);
                }),
                //JdbcExecutionOptions.builder()
                //        .withBatchSize(1000)
                //        .withBatchIntervalMs(200)
                //        .withMaxRetries(5)
                //        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/stu_go?characterEncoding=utf8&useSSL=false")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("rootroot")
                        .build()
        ));

        // job name
        env.execute();

    }


    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {
        Connection conn = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        // open 主要是创建连接
        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/stu_flink",
                    "root", "root");

            // 创建预编译器，有占位符，可传入参数
            insertStmt = conn.prepareStatement("INSERT INTO sensor_temp (id, temp) VALUES ( ?, ?)");
            updateStmt = conn.prepareStatement("UPDATE sensor_temp SET temp = ? WHERE id = ? ");
        }

        // 调用连接，执行 sql
        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            // 执行更新语句，注意不要留 super
            updateStmt.setDouble(1, value.getTemperatrue());
            updateStmt.setString(2, value.getId());
            updateStmt.execute();
            // 如果刚才 update 语句没有更新，那么插入
            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, value.getId());
                insertStmt.setDouble(2, value.getTemperatrue());
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            conn.close();
        }
    }
}
