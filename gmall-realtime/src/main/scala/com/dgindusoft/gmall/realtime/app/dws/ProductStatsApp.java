package com.dgindusoft.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dgindusoft.gmall.realtime.bean.OrderWide;
import com.dgindusoft.gmall.realtime.bean.PaymentWide;
import com.dgindusoft.gmall.realtime.bean.ProductStats;
import com.dgindusoft.gmall.realtime.common.GmallConstant;
import com.dgindusoft.gmall.realtime.func.DimAsyncFunction;
import com.dgindusoft.gmall.realtime.utils.ClickHouseUtil;
import com.dgindusoft.gmall.realtime.utils.DateTimeUtil;
import com.dgindusoft.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStatsApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.基本环境准备
        //Flink流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //设置CK相关参数
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs:///flink/checkpoint1"));
        System.setProperty("HADOOP_USER_NAME", "dgis");

        //TODO 2.从Kafka中获取数据流
        String groupId = "product_stats_app2";
        String pageViewSourceTopic = "dwd_page_log";
        String favorInfoSourceTopic = "dwd_favor_info";
        String cartInfoSourceTopic = "dwd_cart_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        FlinkKafkaConsumer<String> pageLog = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        DataStreamSource<String> pageLogDS = env.addSource(pageLog);

        FlinkKafkaConsumer<String> favorInfo = MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId);
        DataStreamSource<String> favorInfoDS = env.addSource(favorInfo);

        FlinkKafkaConsumer<String> cartInfo = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId);
        DataStreamSource<String> cartInfoDS = env.addSource(cartInfo);

        FlinkKafkaConsumer<String> refundInfo = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId);
        DataStreamSource<String> refundInfoDS = env.addSource(refundInfo);

        FlinkKafkaConsumer<String> commentInfo = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId);
        DataStreamSource<String> commentInfoDS = env.addSource(commentInfo);

        FlinkKafkaConsumer<String> orderWide = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStreamSource<String> orderWideDS = env.addSource(orderWide);

        FlinkKafkaConsumer<String> paymentWide = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId);
        DataStreamSource<String> paymentWideDS = env.addSource(paymentWide);


        //TODO 3.将各个流的数据转换为统一的对象格式
        //点击和曝光
        SingleOutputStreamOperator<ProductStats> productClickAndDispalyDS = pageLogDS.process(
                new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<ProductStats> out) throws Exception {
                        //装换成JSON
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        Long ts = jsonObj.getLong("ts");

                        //点击数据
                        JSONObject page = jsonObj.getJSONObject("page");
                        String pageId = page.getString("page_id");
                        if (pageId != null && pageId.length() > 0 && "good_detail".equals(pageId)) {
                            Long item = page.getLong("item");
                            ProductStats productStats = ProductStats.builder().sku_id(item).click_ct(1L).ts(ts).build();
                            //向下游输出
                            out.collect(productStats);
                        }

                        //曝光数据
                        JSONArray displays = jsonObj.getJSONArray("displays");
                        if (displays != null && displays.size() > 0) {
                            for (int i = 0; i < displays.size(); i++) {
                                JSONObject displayObj = displays.getJSONObject(i);
                                String itemType = displayObj.getString("item_type");
                                if (itemType != null && itemType.length() > 0 && "sku_id".equals(itemType)) {
                                    Long item = displayObj.getLong("item");
                                    ProductStats productStats = ProductStats.builder().sku_id(item).display_ct(1L).ts(ts).build();
                                    //向下游输出
                                    out.collect(productStats);
                                }
                            }
                        }
                    }
                }
        );



        //订单
        SingleOutputStreamOperator<ProductStats> orderWideStatsDS = orderWideDS.map(
                jsonStr -> {
                    OrderWide orderWideBean = JSON.parseObject(jsonStr, OrderWide.class);
                    String createTime = orderWideBean.getCreate_time();
                    Long ts = DateTimeUtil.toTs(createTime);
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(orderWideBean.getSku_id())
                            .order_sku_num(orderWideBean.getSku_num())
                            .order_amount(orderWideBean.getSplit_total_amount())
                            .order_ct(1L)
                            .orderIdSet(new HashSet(Collections.singleton(orderWideBean.getOrder_id())))
                            .ts(ts)
                            .build();

                    return productStats;
                }
        );


        //支付
        SingleOutputStreamOperator<ProductStats> paymentStatsDS = paymentWideDS.map(
                jsonStr -> {
                    PaymentWide paymentWideBean = JSON.parseObject(jsonStr, PaymentWide.class);
                    String payment_create_time = paymentWideBean.getPayment_create_time();
                    Long ts = DateTimeUtil.toTs(payment_create_time);
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(paymentWideBean.getSku_id())
                            .payment_amount(paymentWideBean.getSplit_total_amount())
                            .paid_order_ct(1L)
                            .paidOrderIdSet(new HashSet(Collections.singleton(paymentWideBean.getOrder_id())))
                            .ts(ts)
                            .build();

                    return productStats;
                }
        );


        //收藏
        SingleOutputStreamOperator<ProductStats> favorStatsDS = favorInfoDS.map(
                jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    Long ts = DateTimeUtil.toTs(jsonObj.getString("create_time"));
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(jsonObj.getLong("sku_id"))
                            .favor_ct(1L)
                            .ts(ts)
                            .build();

                    return productStats;
                }
        );


        //加购
        SingleOutputStreamOperator<ProductStats> cartStatsDS = cartInfoDS.map(
                jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    Long ts = DateTimeUtil.toTs(jsonObj.getString("create_time"));
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(jsonObj.getLong("sku_id"))
                            .cart_ct(1L)
                            .ts(ts)
                            .build();

                    return productStats;
                }
        );

        //退款
        SingleOutputStreamOperator<ProductStats> refundStatsDS = refundInfoDS.map(
                jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    Long ts = DateTimeUtil.toTs(jsonObj.getString("create_time"));
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(jsonObj.getLong("sku_id"))
                            .refund_amount(jsonObj.getBigDecimal("refund_amount"))
                            .refund_order_ct(1L)
                            .refundOrderIdSet(new HashSet(Collections.singleton(jsonObj.getLong("order_id"))))
                            .ts(ts)
                            .build();

                    return productStats;
                }
        );

        //评论
        SingleOutputStreamOperator<ProductStats> commonInfoStatsDS = commentInfoDS.map(
                jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    Long ts = DateTimeUtil.toTs(jsonObj.getString("create_time"));
                    Long goodCt = GmallConstant.APPRAISE_GOOD.equals(jsonObj.getString("appraise")) ? 1L : 0L;
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(jsonObj.getLong("sku_id"))
                            .comment_ct(1L)
                            .good_comment_ct(goodCt)
                            .ts(ts)
                            .build();

                    return productStats;
                }
        );


        //TODO 4. 将转换后的流进行合并
        DataStream<ProductStats> unionDS = productClickAndDispalyDS.union(
                orderWideStatsDS,
                paymentStatsDS,
                favorStatsDS,
                cartStatsDS,
                refundStatsDS,
                commonInfoStatsDS
        );

        unionDS.print("union >>>");

        //TODO 5.设置Watermark并且提取事件时间字段
        SingleOutputStreamOperator<ProductStats> productStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(
                        Duration.ofSeconds(5)
                ).withTimestampAssigner(
                        new SerializableTimestampAssigner<ProductStats>() {
                            @Override
                            public long extractTimestamp(ProductStats productStats, long recordTimestamp) {
                                return productStats.getTs();
                            }
                        }
                )
        );

        //TODO 6.按照维度对数据进行分组
        KeyedStream<ProductStats, Long> keyedDS = productStatsWithWatermarkDS.keyBy(data -> data.getSku_id());


        //TODO 7.对分组之后的数据进行开窗   开一个10s的滚动窗口
        WindowedStream<ProductStats, Long, TimeWindow> windowDS = keyedDS.window(
                TumblingEventTimeWindows.of(Time.seconds(10)));


        //TODO 8.对窗口中的元素进行聚合
        SingleOutputStreamOperator<ProductStats> reduceDS = windowDS.reduce(
                new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats productStats1, ProductStats productStats2) throws Exception {
                        productStats1.setDisplay_ct(productStats1.getDisplay_ct() + productStats2.getDisplay_ct());
                        productStats1.setClick_ct(productStats1.getClick_ct() + productStats2.getClick_ct());
                        productStats1.setFavor_ct(productStats1.getFavor_ct() + productStats2.getFavor_ct());
                        productStats1.setCart_ct(productStats1.getCart_ct() + productStats2.getCart_ct());
                        productStats1.setOrder_sku_num(productStats1.getOrder_sku_num() + productStats2.getOrder_sku_num());
                        productStats1.setOrder_amount(productStats1.getOrder_amount().add(productStats2.getOrder_amount()));
                        productStats1.setOrder_ct(productStats1.getOrderIdSet().size() + productStats2.getOrderIdSet().size() + 0L);
                        productStats1.setPayment_amount(productStats1.getPayment_amount().add(productStats2.getPayment_amount()));
                        productStats1.setPaid_order_ct(productStats1.getPaidOrderIdSet().size() + productStats2.getPaidOrderIdSet().size() + 0L);
                        productStats1.setRefund_order_ct(productStats1.getRefundOrderIdSet().size() + productStats2.getRefundOrderIdSet().size() + 0L);
                        productStats1.setRefund_amount(productStats1.getRefund_amount().add(productStats2.getRefund_amount()));
                        productStats1.setComment_ct(productStats1.getComment_ct() + productStats2.getComment_ct());
                        productStats1.setGood_comment_ct(productStats1.getGood_comment_ct() + productStats2.getGood_comment_ct());

                        productStats1.getOrderIdSet().addAll(productStats2.getOrderIdSet());
                        productStats1.getPaidOrderIdSet().addAll(productStats2.getOrderIdSet());
                        productStats1.getRefundOrderIdSet().addAll(productStats2.getRefundOrderIdSet());
                        return productStats1;
                    }
                },
                new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<ProductStats> elements, Collector<ProductStats> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        for (ProductStats element : elements) {
                            element.setStt(sdf.format(new Date(context.window().getStart())));
                            element.setEdt(sdf.format(new Date(context.window().getEnd())));
                            element.setTs(new Date().getTime());
                            out.collect(element);
                        }
                    }
                }
        );

//        reduceDS.print("reduce>>>");

        //TODO 9.补充商品的维度信息
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {

                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                        productStats.setSku_name(dimInfoJsonObj.getString("SKU_NAME"));
                        productStats.setSku_price(dimInfoJsonObj.getBigDecimal("PRICE"));
                        productStats.setSpu_id(dimInfoJsonObj.getLong("SPU_ID"));
                        productStats.setTm_id(dimInfoJsonObj.getLong("TM_ID"));
                        productStats.setCategory3_id(dimInfoJsonObj.getLong("CATEGORY3_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS);

        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(
                productStatsWithSkuDS,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {

                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSpu_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                        productStats.setSpu_name(dimInfoJsonObj.getString("SPU_NAME"));

                    }
                },
                60,
                TimeUnit.SECONDS);


        SingleOutputStreamOperator<ProductStats> productStatsWithTMDS = AsyncDataStream.unorderedWait(
                productStatsWithSpuDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {

                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getCategory3_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                        productStats.setSpu_name(dimInfoJsonObj.getString("TM_NAME"));

                    }
                },
                60,
                TimeUnit.SECONDS);


        SingleOutputStreamOperator<ProductStats> productStatsWithCategoryDS = AsyncDataStream.unorderedWait(
                productStatsWithTMDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getCategory3_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                        productStats.setCategory3_name(dimInfoJsonObj.getString("NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );


        //TODO 10.将聚合后的流数据写到ClickHouse中
        productStatsWithCategoryDS.addSink(
                ClickHouseUtil.<ProductStats>getJdbcSink("insert into product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        );

        productStatsWithCategoryDS.print(">>>");

        //TODO 11.将统计的结果写回到kafka的dws层


        env.execute();
    }
}
