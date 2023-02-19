package com.pcy.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author PengChenyu
 * @since 2022-09-09 22:00:15
 */
public class SourceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.从文件中读取数据
        DataStreamSource<String> textStream = env.readTextFile("input/word.txt");
        textStream.print("textStream");

        // 2.从集合中读取数据
<<<<<<< HEAD
        ArrayList<Event> clicks = new ArrayList<Event>();
=======
        ArrayList<Event> clicks = new ArrayList<>();
>>>>>>> 124b9030657e5021c1d90ba09373c7523129681c
        clicks.add(new Event("Mary", "./home", 1000L));
        clicks.add(new Event("Bob", "./cart", 2000L));
        DataStream<Event> colStream1 = env.fromCollection(clicks);
        colStream1.print("colStream1");
        DataStreamSource<Event> colStream2 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );
        colStream2.print("colStream2");


        env.execute();
    }
}
