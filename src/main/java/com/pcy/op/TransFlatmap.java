package com.pcy.op;

import com.pcy.source.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransFlatmap {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("mary", "./home", 1000L),
                new Event("bob", "./home", 1000L)
        );

        stream.flatMap(new MyFlatMap()).print();
        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<Event, String> {

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            if (event.user.equals("mary")){
                collector.collect(event.user);
            }else if(event.user.equals("bob")){
                collector.collect(event.user);
                collector.collect(event.url);
            }
        }
    }
}
