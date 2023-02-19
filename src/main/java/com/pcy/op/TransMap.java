package com.pcy.op;

import com.pcy.source.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransMap {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("mary", "./home", 1000L),
                new Event("bob", "./home", 1000L)
        );
        // 传入匿名类
        stream.map((MapFunction<Event, String>) event -> event.user).print();

        // 传入MapFunction的实现类
        stream.map(new UserExtractor()).print();
        env.execute();
    }

    public static class UserExtractor implements MapFunction<Event, String>{
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}
