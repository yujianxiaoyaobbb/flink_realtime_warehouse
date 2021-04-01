package com.dgindusoft.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dgindusoft.gmall.realtime.bean.OrderDetail;
import com.dgindusoft.gmall.realtime.bean.OrderInfo;
import com.dgindusoft.gmall.realtime.bean.OrderWide;
import com.dgindusoft.gmall.realtime.func.DimAsyncFunction;
import com.dgindusoft.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {

        //TODO 0.基本环境准备
        //Flink流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //设置CK相关参数
        /*env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);*/
        /*env.getCheckpointConfig().setCheckpointTimeout(60000);*/
        /*env.setStateBackend(new FsStateBackend("hdfs:///flink/checkpoint1"));*/
        /*System.setProperty("HADOOP_USER_NAME", "dgis");*/

        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group8";

        //读取OrderInfo和OrderDetail
        FlinkKafkaConsumer<String> orderInfoSource = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);

        DataStreamSource<String> orderInfoDS = env.addSource(orderInfoSource);
        DataStreamSource<String> orderDetailDS = env.addSource(orderDetailSource);


        //TODO 1.转换结构
        SingleOutputStreamOperator<OrderInfo> orderInfoWithTsDS = orderInfoDS.map(
                new RichMapFunction<String, OrderInfo>() {

                    private SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderInfo map(String jsonStr) throws Exception {
                        OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                        long ts = sdf.parse(orderInfo.getCreate_time()).getTime();
                        orderInfo.setCreate_ts(ts);
                        return orderInfo;
                    }
                }
        );


        SingleOutputStreamOperator<OrderDetail> orderDetailWithTsDS = orderDetailDS.map(
                new RichMapFunction<String, OrderDetail>() {

                    private SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    }

                    @Override
                    public OrderDetail map(String jsonStr) throws Exception {
                        OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                        long ts = sdf.parse(orderDetail.getCreate_time()).getTime();
                        orderDetail.setCreate_ts(ts);
                        return orderDetail;
                    }
                }
        );


        //TODO 2. 指定事件时间字段
        SingleOutputStreamOperator<OrderInfo> orderInfoWithEventTsDS = orderInfoWithTsDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                                return orderInfo.getCreate_ts();
                            }
                        })
        );

        SingleOutputStreamOperator<OrderDetail> orderDetailWithEventTsDS = orderDetailWithTsDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
                                return orderDetail.getCreate_ts();
                            }
                        })
        );



        //TODO 3.进行双流join
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoWithEventTsDS
                .keyBy(data -> data.getId())
                .intervalJoin(
                        orderDetailWithEventTsDS
                                .keyBy(data -> data.getOrder_id())
                ).between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));

                    }
                });

//        orderWideDS.print(">>>");

        //TODO 4.和用户维度表进行join
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
                orderWideDS
                , new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                        String birthday = dimInfoJsonObj.getString("BIRTHDAY");
                        long current = System.currentTimeMillis();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        Date birth = sdf.parse(birthday);
                        long birthTs = birth.getTime();
                        Long ageLong = (current - birthTs) / 3600 / 24 / 365 / 1000;
                        int age = ageLong.intValue();
                        orderWide.setUser_age(age);
                        orderWide.setUser_gender(dimInfoJsonObj.getString("GENDER"));
                    }
                }
                , 60
                , TimeUnit.SECONDS
        );

        //TODO 5.和省市维度表进行join
        SingleOutputStreamOperator<OrderWide> orderWideWithUserProvinceDS = AsyncDataStream.unorderedWait(
                orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {

                    @Override
                    public String getKey(OrderWide orderWide) {

                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                        orderWide.setProvince_name(dimInfoJsonObj.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfoJsonObj.getString("AREA_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfoJsonObj.getString("ISO_3166_2"));
                        orderWide.setProvince_iso_code(dimInfoJsonObj.getString("ISO_CODE"));
                    }
                }
                , 60
                , TimeUnit.SECONDS
        );


        //TODO 6.关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserAndProvinceSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithUserProvinceDS
                , new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                        orderWide.setSku_name(dimInfoJsonObj.getString("SKU_NAME"));
                        orderWide.setSpu_id(dimInfoJsonObj.getLong("SPU_ID"));
                        orderWide.setCategory3_id(dimInfoJsonObj.getLong("CATEGORY3_ID"));
                        orderWide.setTm_id(dimInfoJsonObj.getLong("TM_ID"));
                    }
                }
                , 60
                , TimeUnit.SECONDS
        );

        //TODO 7.关联SPU商品维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserAndProvinceSkuSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithUserAndProvinceSkuDS
                , new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {

                    @Override
                    public String getKey(OrderWide orderWide) {

                        return orderWide.getSpu_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                        orderWide.setSpu_name(dimInfoJsonObj.getString("SPU_NAME"));
                    }
                }
                , 60
                , TimeUnit.SECONDS
        );

        //TODO 8.关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserAndProvinceSkuSpuCateDS = AsyncDataStream.unorderedWait(
                orderWideWithUserAndProvinceSkuSpuDS
                , new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {

                    @Override
                    public String getKey(OrderWide orderWide) {

                        return orderWide.getCategory3_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                        orderWide.setCategory3_name(dimInfoJsonObj.getString("NAME"));
                    }
                }
                , 60
                , TimeUnit.SECONDS
        );

        //TODO 9.关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserAndProvinceSkuSpuCateTmDS = AsyncDataStream.unorderedWait(
                orderWideWithUserAndProvinceSkuSpuCateDS
                , new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {

                    @Override
                    public String getKey(OrderWide orderWide) {

                        return orderWide.getTm_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                        orderWide.setTm_name(dimInfoJsonObj.getString("TM_NAME"));
                    }
                }
                , 60
                , TimeUnit.SECONDS
        );

        orderWideWithUserAndProvinceSkuSpuCateTmDS.print(">>>");
        //TODO 10.写入到kafka
        FlinkKafkaProducer<String> kafkaSink = MyKafkaUtil.getKafkaSink(orderWideSinkTopic);
        orderWideWithUserAndProvinceSkuSpuCateTmDS
                .map(data -> JSON.toJSONString(data))
                .addSink(kafkaSink);

        env.execute();

    }
}
