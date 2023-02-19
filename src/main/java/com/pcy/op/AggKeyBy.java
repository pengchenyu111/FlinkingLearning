package com.pcy.op;

import com.pcy.source.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggKeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 使用Lambda
        KeyedStream<Event, String> keyedStream = stream.keyBy(event -> event.user);

        // 使用匿名类
        KeyedStream<Event, String> keyedStream1 = stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        });

        env.execute();

    }
}
