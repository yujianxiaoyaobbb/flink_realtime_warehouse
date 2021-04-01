package com.dgindusoft.gmall.realtime.app.dws;

import com.dgindusoft.gmall.realtime.bean.KeywordStats;
import com.dgindusoft.gmall.realtime.common.GmallConstant;
import com.dgindusoft.gmall.realtime.func.KeywordUDTF;
import com.dgindusoft.gmall.realtime.utils.ClickHouseUtil;
import com.dgindusoft.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {

        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //设置CK相关参数
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs:///flink/checkpoint1"));
        System.setProperty("HADOOP_USER_NAME", "dgis");

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
        //TODO 2.注册自定义函数
        tableEnvironment.createTemporaryFunction("ik_analyze", KeywordUDTF.class);

        //TODO 3.创建动态表
        String pageViewSourceTopic = "dwd_page_log";
        String groupId = "keywordstats_app_group";

        tableEnvironment.executeSql(
                "CREATE TABLE page_view (" +
                        " common MAP<STRING, STRING>," +
                        " page MAP<STRING, STRING>," +
                        " ts BIGINT," +
                        " rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                        " WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                        " WITH (" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")"
        );

        //TODO 4.从动态表中查询数据  --->尚硅谷大数据数仓-> [尚, 硅谷, 大, 数据, 数, 仓]
        Table fullwordTable = tableEnvironment.sqlQuery(
                "select page['item'] fullword,rowtime " +
                        " from page_view " +
                        " where page['page_id']='good_list' and page['item'] IS NOT NULL"
        );

        //TODO 5.利用自定义函数  对搜索关键词进行拆分
        Table keywordTable = tableEnvironment.sqlQuery(
                "SELECT keyword, rowtime " +
                        "FROM  " + fullwordTable + "," +
                        "LATERAL TABLE(ik_analyze(fullword)) AS t(keyword)"
        );

        //TODO 6.分组、开窗、聚合
        Table reduceTable = tableEnvironment.sqlQuery(
                "select keyword,count(*) ct,  '" + GmallConstant.KEYWORD_SEARCH + "' source," +
                        "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                        "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt ," +
                        "UNIX_TIMESTAMP()*1000 ts from " + keywordTable +
                        " group by TUMBLE(rowtime, INTERVAL '10' SECOND),keyword"
        );

        //TODO 7.转换为流
        DataStream<KeywordStats> keywordStatsDS = tableEnvironment.toAppendStream(reduceTable, KeywordStats.class);

        keywordStatsDS.print(">>>");

        //TODO 8.写入到ClickHouse
        keywordStatsDS.addSink(
                ClickHouseUtil.getJdbcSink("insert into keyword_stats(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)")
        );

        env.execute();
    }
}
