package com.dgindusoft.gmall.realtime.common;

public class GmallConfig {
    public static final String HBASE_SCHEMA="GMALL_REALTIME";
    public static final String PHOENIX_SERVER="jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181";

    //ClickHouse的URL连接地址
    public static final String CLICKHOUSE_URL="jdbc:clickhouse://hadoop101:8123/default";

}
