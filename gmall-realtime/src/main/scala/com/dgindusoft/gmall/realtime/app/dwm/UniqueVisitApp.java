package com.dgindusoft.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dgindusoft.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.text.SimpleDateFormat;
import java.util.Date;

public class UniqueVisitApp {

    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
        //Flink流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //设置CK相关参数
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs:///flink/checkpoint1"));
        System.setProperty("HADOOP_USER_NAME", "dgis");


        String sourceTopic = "dwd_page_log";
        String groupId = "unique_visit_app_group";
        String sinkTopic = "dwm_unique_visit";
        //TODO 1.读取kafka数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);

        DataStreamSource<String> sourceDS = env.addSource(kafkaSource);

        //TODO 2.对读取的kafka数据进行结构转换并按照mid进行分组
        SingleOutputStreamOperator<JSONObject> mapDS = sourceDS.map(data -> JSON.parseObject(data));

        KeyedStream<JSONObject, String> keyedDS = mapDS
                .keyBy(data -> data.getJSONObject("common")
                        .getString("mid"));

        //进行UV过滤
        SingleOutputStreamOperator<JSONObject> filterDS = keyedDS.filter(
                new RichFilterFunction<JSONObject>() {

                    private SimpleDateFormat sdf = null;
                    //状态变量
                    ValueState<String> lastVisitDateState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyyMMdd");
                        //初始化状态变量
                        ValueStateDescriptor<String> lastVisitDateStateDesc = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        this.lastVisitDateState = getRuntimeContext().getState(lastVisitDateStateDesc);

                        //因为我们统计的是日活DAU，所以状态数据只在当天有效 ，过了一天就可以失效掉
                        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();
                        lastVisitDateStateDesc.enableTimeToLive(stateTtlConfig);
                    }

                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
//                        System.out.println("**************************" + lastVisitDateState.value() + "**************************");
                        String last_page_id = jsonObj.getJSONObject("page").getString("last_page_id");
                        //如果是其他页面跳转过来的，则过滤
                        if (last_page_id != null) {
                            return false;
                        }
                        //比较当前访问页面的时间和首次访问页面的时间
                        Long ts = Long.parseLong(jsonObj.getString("ts"));
                        String currentDate = sdf.format(new Date(ts));
                        String lastVisitDate = lastVisitDateState.value();
                        if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(currentDate)) {
                            System.out.println("已访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + currentDate);
                            return false;
                        } else {
                            System.out.println("未访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + currentDate);
                            lastVisitDateState.update(currentDate);
                            return true;
                        }
                    }
                }
        );


        //TODO 3.写入到kafka
        SingleOutputStreamOperator<String> filterMapDS = filterDS.map(data -> data.toString());

        filterMapDS.print("DWM UV>>>");
        FlinkKafkaProducer<String> kafkaSink = MyKafkaUtil.getKafkaSink(sinkTopic);
        filterMapDS.addSink(kafkaSink);

        //执行
        env.execute();
    }
}
