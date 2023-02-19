package com.pcy.source;

import java.sql.Timestamp;

/**
 * 注意:
 * 	类是公有（public）的
 * 	有一个无参的构造方法
 * 	所有属性都是公有（public）的
 * 	所有属性的类型都是可以序列化的
 * <p>
 * Flink会把这样的类作为一种特殊的POJO数据类型来对待，方便数据的解析和序列化。
 *
 * @author PengChenyu
 * @since 2022-09-09 21:48:36
 */
public class Event {
    public String user;
    public String url;
    public Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
