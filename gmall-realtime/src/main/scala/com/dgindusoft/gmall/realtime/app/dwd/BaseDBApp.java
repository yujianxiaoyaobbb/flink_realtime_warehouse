package com.dgindusoft.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dgindusoft.gmall.realtime.func.DimSink;
import com.dgindusoft.gmall.realtime.func.TableProcessFunction;
import com.dgindusoft.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDBApp {
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

        //重启策略
//        env.setRestartStrategy(RestartStrategies.noRestart());

        //TODO 1.接收Kafka数据，过滤空值数据
        //定义消费者组以及指定消费主题
        String topic = "ods_base_db_m";
        String groupId = "ods_base_group";

        //从Kafka主题中读取数据
        FlinkKafkaConsumer<String> kafkaSource  = MyKafkaUtil.getKafkaSource(topic,groupId);
        DataStream<String> jsonDstream   = env.addSource(kafkaSource);
        //jsonDstream.print("data json:::::::");

        //对数据进行结构的转换   String->JSONObject
        DataStream<JSONObject>  jsonStream   = jsonDstream.map(jsonStr -> JSON.parseObject(jsonStr));
        //DataStream<JSONObject>  jsonStream   = jsonDstream.map(JSON::parseObject);

        //过滤为空或者 长度不足的数据
        SingleOutputStreamOperator<JSONObject> filteredDstream = jsonStream.filter(
                jsonObject -> {
                    boolean flag = jsonObject.getString("table") != null
                            && jsonObject.getJSONObject("data") != null
                            && jsonObject.getString("data").length() > 3;
                    return flag;
                }) ;

//        filteredDstream.print(">>>fs");
//        filteredDstream.print("json::::::::");

        //定义侧输出流标签
        OutputTag<JSONObject> hbasePutTag = new OutputTag<JSONObject>("hbase") {};

        //进行分流处理
        SingleOutputStreamOperator<JSONObject> kafkaDS = filteredDstream.process(new TableProcessFunction(hbasePutTag));

        //获取侧输出流
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbasePutTag);

        //TODO 将侧输出流写入到Phoenix
        hbaseDS.addSink(new DimSink());

        //TODO 将流输出到kafka
        FlinkKafkaProducer<JSONObject> kafkaSink = MyKafkaUtil.getKafkaSinkBySchema(
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                        String sinkTopic = jsonObject.getString("sink_table");
                        JSONObject data = jsonObject.getJSONObject("data");
                        return new ProducerRecord<>(sinkTopic, data.toString().getBytes());
                    }
                }
        );
        kafkaDS.addSink(kafkaSink);

        kafkaDS.print("kafka>>>");


        env.execute("");

    }
}
