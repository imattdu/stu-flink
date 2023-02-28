package com.matt.apitest.model;

import java.sql.Timestamp;

public class UrlCntBO {

    public String url;
    public Long cnt;
    public Long winStart;
    public Long winEnd;

    public UrlCntBO(String url, Long cnt, Long winStart, Long winEnd) {
        this.url = url;
        this.cnt = cnt;
        this.winStart = winStart;
        this.winEnd = winEnd;
    }

    public UrlCntBO() {
    }

    @Override
    public String toString() {
        return "UrlCntBO{" +
                "url='" + url + '\'' +
                ", cnt=" + cnt +
                ", winStart=" + new Timestamp(winStart) +
                ", winEnd=" + new Timestamp(winEnd) +
                '}';
    }
}
