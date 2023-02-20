package com.pcy.partition;

import com.pcy.source.ClickSource;
import com.pcy.source.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class PartitionMethod {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 读取数据源，并行度为 1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        //1. shuffle
        stream.shuffle().print("shuffle").setParallelism(4);

        // 2.Round-Robin 轮询分区
        stream.rebalance().print("rebalance").setParallelism(4);

        // 3. rescale 重缩放分区
        env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                for (int i = 0; i < 8; i++) {
                    // 将奇数发送到索引为 1的并行子任务
                    // 将偶数发送到索引为 0的并行子任务
                    if ((i + 1) % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        sourceContext.collect(i + 1);
                    }
                }
            }

            @Override
            public void cancel() {
            }
        }).setParallelism(2).rescale().print().setParallelism(4);

        // 4.broadcast 经过广播之后，数据会在不同的分区都保留一 份，可能进行重复处理
        stream.broadcast().print("broadcast").setParallelism(4);

         // 5.global  将所有的输入流数据都发送到下游算子的第一个并行子任务中去。这就相当于强行让下游任务并行度变成了 1
        stream.global().print();

        // 6. 自定义分区
        // // 将自然数按照奇偶分区
        env.fromElements(1,2,3,4,5,6,7,8,9).partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                return key % 2;
            }
        }, new KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer value) throws Exception {
                return value;
            }
        }).print().setParallelism(2);


        env.execute();
    }
}
