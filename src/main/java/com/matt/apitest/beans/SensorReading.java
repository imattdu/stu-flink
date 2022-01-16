package com.matt.apitest.beans;

import java.util.function.DoubleToLongFunction;

/**
 * @author matt
 * @create 2022-01-16 14:38
 */
// 传感器温度读数的数据类型
public class SensorReading {
    private String id;
    private long timestamp;
    private Double temperatrue;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemperatrue() {
        return temperatrue;
    }

    public void setTemperatrue(Double temperatrue) {
        this.temperatrue = temperatrue;
    }


    public SensorReading() {
    }

    public SensorReading(String id, long timestamp, Double temperatrue) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperatrue = temperatrue;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperatrue=" + temperatrue +
                '}';
    }
}
