package com.dgindusoft.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dgindusoft.gmall.realtime.bean.VisitorStats;
import com.dgindusoft.gmall.realtime.utils.ClickHouseUtil;
import com.dgindusoft.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {

        //TODO 0.准备环境
        //Flink流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //设置CK相关参数
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs:///flink/checkpoint1"));
        System.setProperty("HADOOP_USER_NAME", "dgis");

        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String groupId = "visitor_stats_app";

        //TODO 1.读取数据
        FlinkKafkaConsumer<String> pvKafkaSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> uvKafkaSource = MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId);
        FlinkKafkaConsumer<String> ujKafkaSource = MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId);

        DataStreamSource<String> pvDS = env.addSource(pvKafkaSource);
        DataStreamSource<String> uvDS = env.addSource(uvKafkaSource);
        DataStreamSource<String> ujDS = env.addSource(ujKafkaSource);


        //TODO 2.对读取的流进行结构转换
        SingleOutputStreamOperator<VisitorStats> pvVisitorStatsDS = pvDS.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {

                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                jsonObject.getJSONObject("common").getString("vc"),
                                jsonObject.getJSONObject("common").getString("ch"),
                                jsonObject.getJSONObject("common").getString("ar"),
                                jsonObject.getJSONObject("common").getString("is_new"),
                                0L,
                                1L,
                                0L,
                                0L,
                                jsonObject.getJSONObject("page").getLong("during_time"),
                                jsonObject.getLong("ts")
                        );
                        return visitorStats;
                    }
                }
        );


        SingleOutputStreamOperator<VisitorStats> uvVisitorStatsDS = uvDS.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                jsonObject.getJSONObject("common").getString("vc"),
                                jsonObject.getJSONObject("common").getString("ch"),
                                jsonObject.getJSONObject("common").getString("ar"),
                                jsonObject.getJSONObject("common").getString("is_new"),
                                1L,
                                0L,
                                0L,
                                0L,
                                0L,
                                jsonObject.getLong("ts")
                        );
                        return visitorStats;
                    }
                }
        );

        SingleOutputStreamOperator<VisitorStats> ujVisitorStatsDS = ujDS.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                jsonObject.getJSONObject("common").getString("vc"),
                                jsonObject.getJSONObject("common").getString("ch"),
                                jsonObject.getJSONObject("common").getString("ar"),
                                jsonObject.getJSONObject("common").getString("is_new"),
                                0L,
                                0L,
                                0L,
                                1L,
                                0L,
                                jsonObject.getLong("ts")
                        );
                        return visitorStats;
                    }
                }
        );

        SingleOutputStreamOperator<VisitorStats> svVisitorStatsDS = pvDS.process(
                new ProcessFunction<String, VisitorStats>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<VisitorStats> out) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        try{
                            String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                            if (lastPageId == null && lastPageId.length() == 0) {
                                VisitorStats visitorStats = new VisitorStats(
                                        "",
                                        "",
                                        jsonObject.getJSONObject("common").getString("vc"),
                                        jsonObject.getJSONObject("common").getString("ch"),
                                        jsonObject.getJSONObject("common").getString("ar"),
                                        jsonObject.getJSONObject("common").getString("is_new"),
                                        0L,
                                        0L,
                                        1L,
                                        0L,
                                        0L,
                                        jsonObject.getLong("ts")
                                );
                                out.collect(visitorStats);
                            }
                        }catch (Exception e){
                            System.out.println("首页");
                        }

                    }
                }
        );

        //TODO 3.对读取的流进行合并
        DataStream<VisitorStats> unionDS = pvVisitorStatsDS.union(uvVisitorStatsDS, svVisitorStatsDS, ujVisitorStatsDS);

        //TODO 4.设置时间语义、watermark
        SingleOutputStreamOperator<VisitorStats> visitorStatsTsDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<VisitorStats>() {
                                    @Override
                                    public long extractTimestamp(VisitorStats visitorStats, long recordTimestamp) {

                                        return visitorStats.getTs();
                                    }
                                }
                        )
        );

        //TODO 5.进行开窗
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedDS = visitorStatsTsDS.keyBy(
                new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                        return Tuple4.of(
                                visitorStats.getAr(),
                                visitorStats.getCh(),
                                visitorStats.getVc(),
                                visitorStats.getIs_new()
                        );
                    }
                }
        );

        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = keyedDS.window(
                TumblingEventTimeWindows.of(Time.seconds(10))
        );

        //TODO 6.对窗口数据进行聚合
        SingleOutputStreamOperator<VisitorStats> reduceDS = windowDS.reduce(
                new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats visitorStats1, VisitorStats visitorStats2) throws Exception {
                        visitorStats1.setPv_ct(visitorStats1.getPv_ct() + visitorStats2.getPv_ct());
                        visitorStats1.setUv_ct(visitorStats1.getUv_ct() + visitorStats2.getUv_ct());
                        visitorStats1.setSv_ct(visitorStats1.getSv_ct() + visitorStats2.getSv_ct());
                        visitorStats1.setUj_ct(visitorStats1.getUj_ct() + visitorStats2.getUj_ct());
                        visitorStats1.setDur_sum(visitorStats1.getDur_sum() + visitorStats2.getDur_sum());
                        return visitorStats1;
                    }
                },
                new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {

                    @Override
                    public void process(Tuple4<String, String, String, String> key, Context context, Iterable<VisitorStats> elements, Collector<VisitorStats> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        for (VisitorStats visitorStats : elements) {
                            String start = sdf.format(new Date(context.window().getStart()));
                            String end = sdf.format(new Date(context.window().getEnd()));
                            visitorStats.setStt(start);
                            visitorStats.setEdt(end);
                            out.collect(visitorStats);
                        }
                    }
                }
        );

        reduceDS.print(">>>");
        //TODO 7.写入到clickhouse
        reduceDS.addSink(
                ClickHouseUtil.getJdbcSink("insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)")
        );
        //TODO 8.执行
        env.execute();
    }
}
