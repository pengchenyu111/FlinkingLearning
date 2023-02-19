package com.pcy.op;

import com.pcy.source.ClickSource;
import com.pcy.source.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggReduce {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env.addSource(new ClickSource())
                .map(e -> Tuple2.of(e.user, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(r -> r.f0) // 使用用户名来进行分流
                .reduce((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1))
                .keyBy(r -> true)
                .reduce((value1, value2) -> value1.f1 > value2.f1 ? value1 : value2)
                .print();


        env.execute();

    }
}
