package com.dgindusoft.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.dgindusoft.gmall.realtime.bean.OrderWide;
import com.dgindusoft.gmall.realtime.bean.PaymentInfo;
import com.dgindusoft.gmall.realtime.bean.PaymentWide;
import com.dgindusoft.gmall.realtime.utils.DateTimeUtil;
import com.dgindusoft.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 0.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(4);
        //检查点相关的配置
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs:///flink/checkpoint1"));
        System.setProperty("HADOOP_USER_NAME", "dgis");

        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        String groupId = "paymentwide_app_group3";

        //TODO 1.读取kafka数据
        DataStreamSource<String> payMentSourceDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId));

        DataStreamSource<String> orderWideSourceDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));


        SingleOutputStreamOperator<PaymentInfo> payMentInfoDS = payMentSourceDS.map(data -> JSON.parseObject(data, PaymentInfo.class));

        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideSourceDS.map(data -> JSON.parseObject(data, OrderWide.class));


        //TODO 2.设置时间语义&抽取事件时间&设置watermark
        SingleOutputStreamOperator<PaymentInfo> payMentInfoTsDS = payMentInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<PaymentInfo>() {
                                    @Override
                                    public long extractTimestamp(PaymentInfo paymentInfo, long recordTimestamp) {
                                        return DateTimeUtil.toTs(paymentInfo.getCallback_time());
                                    }
                                }
                        )
        );


        SingleOutputStreamOperator<OrderWide> orderWideTsDS = orderWideDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide orderWide, long recordTimestamp) {

                                return DateTimeUtil.toTs(orderWide.getCreate_time());
                            }
                        })
        );

        //TODO 3.双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = payMentInfoTsDS.keyBy(data -> data.getOrder_id())
                .intervalJoin(
                        orderWideTsDS.keyBy(data -> data.getOrder_id())
                ).between(Time.seconds(-1800), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                });



        paymentWideDS.print("paymentWideDS>>>");
        //TODO 4.写出到kafka
        FlinkKafkaProducer<String> kafkaSink = MyKafkaUtil.getKafkaSink(paymentWideSinkTopic);
        paymentWideDS.map(data -> JSON.toJSONString(data))
                .addSink(kafkaSink);


        env.execute();
    }
}
