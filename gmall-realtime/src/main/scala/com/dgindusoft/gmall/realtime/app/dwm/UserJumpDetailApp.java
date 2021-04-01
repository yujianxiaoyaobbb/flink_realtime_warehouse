package com.dgindusoft.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dgindusoft.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;


public class UserJumpDetailApp {

    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1  准备本地测试流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.2 设置并行度
        env.setParallelism(4);

        //设置CK相关参数
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs:///flink/checkpoint1"));
        System.setProperty("HADOOP_USER_NAME", "dgis");

        //TODO 2.从kafka中读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "user_jump_detail_group";
        String sinkTopic = "dwm_user_jump_detail";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> dataStream = env.addSource(kafkaSource);

        /*DataStream<String> dataStream1 = env
                .fromElements(
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"home\"},\"ts\":150000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"detail\"},\"ts\":300000} "
                );*/

        SingleOutputStreamOperator<JSONObject> jsonObj = dataStream.map(data -> JSON.parseObject(data));
        //设置时间语义
        SingleOutputStreamOperator<JSONObject> jsonObjWithTSDS = jsonObj.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(
                new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                        return jsonObj.getLong("ts");
                    }
                }
        ));

        //TODO 3.按照mid进行分组
        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = jsonObjWithTSDS.keyBy(data -> data.getJSONObject("common")
                .getString("mid"));



        //TODO 4.配置CEP
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first")
                //模式1:不是从其它页面跳转过来的页面，是一个首次访问页面
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String last_page_id = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (last_page_id == null || last_page_id.length() == 0) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                }).next("next")
                //模式2. 判读是否对页面做了访问
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String page_id = jsonObj.getJSONObject("page").getString("page_id");
                        if (page_id != null && page_id.length() > 0) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                }).within(Time.milliseconds(10000));

        PatternStream<JSONObject> patternStream = CEP.pattern(jsonObjectStringKeyedStream, pattern);

        //侧输出
        OutputTag<String> timeout = new OutputTag<String>("timeout"){};

        SingleOutputStreamOperator<String> filterDS = patternStream.flatSelect(
                timeout,
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                        List<JSONObject> jsonObjectList = pattern.get("first");
                        for (JSONObject jsonObject : jsonObjectList) {
                            out.collect(JSON.toJSONString(jsonObject));
                        }

                    }
                },
                new PatternFlatSelectFunction<JSONObject, String>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<String> out) throws Exception {

                    }
                }
        );

        DataStream<String> jumpDS = filterDS.getSideOutput(timeout);

        jumpDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));
        jumpDS.print(">>>");

        env.execute();

    }
}
