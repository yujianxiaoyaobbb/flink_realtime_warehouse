package com.dgindusoft.gmall.realtime.app.dwd;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lineDS = env.socketTextStream("hadoop101",9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = lineDS.map(data -> Tuple2.of(data, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        SingleOutputStreamOperator<String> flatMapDS = lineDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(s);
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDS = flatMapDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });


        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = mapDS.keyBy(data -> data.f0).sum(1);

        resultDS.print();
        env.execute();
    }
}
