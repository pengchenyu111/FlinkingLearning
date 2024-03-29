package com.pcy.op;

import com.pcy.source.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFuncOp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L)
        );


        clicks.map(new RichMapFunction<Event, Long>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("开始，索引为：" + getRuntimeContext().getIndexOfThisSubtask());
            }

            @Override
            public Long map(Event value) throws Exception {
                return value.timestamp;
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("结束，索引为：" + getRuntimeContext().getIndexOfThisSubtask());
            }
        }).print();

        env.execute();


    }
}
