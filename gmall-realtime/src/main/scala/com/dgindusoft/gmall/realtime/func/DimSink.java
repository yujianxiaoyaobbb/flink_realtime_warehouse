package com.dgindusoft.gmall.realtime.func;

import com.alibaba.fastjson.JSONObject;
import com.dgindusoft.gmall.realtime.common.GmallConfig;
import com.dgindusoft.gmall.realtime.utils.DimUtil;
import com.dgindusoft.gmall.realtime.utils.RedisUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

public class DimSink extends RichSinkFunction<JSONObject> {

    private Connection conn = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        //获取连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        String phoenixTable = jsonObj.getString("sink_table");
        JSONObject data = jsonObj.getJSONObject("data");
        //拼接SQL
        String genUpsertSql = genUpsertSql(phoenixTable.toUpperCase(), data);
        //打印拼接的SQL
        System.out.println(genUpsertSql);
        //执行SQL
        PreparedStatement ps = null;
        //如果是操作类型是update，则将redis中的key删除
        if(jsonObj.getString("type").equals("update")){
            DimUtil.deleteCached(phoenixTable,data.getString("id"));
        }
        try{
            ps = conn.prepareStatement(genUpsertSql);
            ps.execute();
            //执行完提交事务
            conn.commit();
        }catch (SQLException e){
            e.printStackTrace();
            throw new RuntimeException("向Phoenix插入数据失败");
        }finally {
            if(ps != null){
                ps.close();
            }
        }

    }

    private String genUpsertSql(String toUpperCase, JSONObject data) {
        StringBuffer sb = new StringBuffer("upsert into " + GmallConfig.HBASE_SCHEMA + "." + toUpperCase);
        StringBuffer name = new StringBuffer("(");
        StringBuffer value = new StringBuffer(" values (");
        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
        for (;iterator.hasNext();) {
            Map.Entry<String, Object> entry = iterator.next();
            name.append(entry.getKey().toUpperCase()).append(",");
            value.append("'").append(entry.getValue()).append("'").append(",");
        }
        name.deleteCharAt(name.length()-1);
        value.deleteCharAt(value.length()-1);
        name.append(")");
        value.append(")");
        return sb.append(name).append(value).toString();
    }
}
