package com.dgindusoft.gmall.realtime.func;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dgindusoft.gmall.realtime.bean.TableProcess;
import com.dgindusoft.gmall.realtime.common.GmallConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import static com.dgindusoft.gmall.realtime.utils.MySQLUtil.queryList;

public class TableProcessFunction extends ProcessFunction<JSONObject,JSONObject> {

    //侧输出流标签
    private OutputTag<JSONObject> outputTag;
    //存放表配置的集合
    private HashMap<String, TableProcess> tableProcessMap = new HashMap<>();
    //存放hbase中已存在表的集合
    private HashSet<String> existsTables = new HashSet<>();

    //Phooenix连接
    private Connection connection = null;

    public TableProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    //开启时处理
    @Override
    public void open(Configuration parameters) throws Exception {

//        System.out.println("-----------------进入到open-----------------");
        //获取Phoenix连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        //第一次初始化存放表配置的集合
        initTableProcessMap();
        //进行定时
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                initTableProcessMap();
            }
        }, 5000, 5000);

    }

    private void initTableProcessMap() {
//        System.out.println("------------------进入到initTableProcessMap------------------");
        List<TableProcess> tableProcesses = queryList("select * from table_process", TableProcess.class, true);
        //遍历得到的集合,并更新到配置集合中
        for (TableProcess tableProcess : tableProcesses) {
            String operateType = tableProcess.getOperateType();
            String sinkColumns = tableProcess.getSinkColumns();
            String sinkExtend = tableProcess.getSinkExtend();
            String sinkPk = tableProcess.getSinkPk();
            String sourceTable = tableProcess.getSourceTable();
            String sinkType = tableProcess.getSinkType();
            String sinkTable = tableProcess.getSinkTable();
            String rk = sourceTable + "_" + operateType;
            tableProcessMap.put(rk,tableProcess);

//            System.out.println("--------------------"+tableProcess+"--------------------");

            //更新已存在的hbase表,若表不存在则建表
            if("hbase".equals(sinkType) && "insert".equals(operateType)){
                boolean isExist = existsTables.add(sinkTable);
                if(isExist){
                    //如果插入已存在的表成功，即已存在的表中没有这个新添加的表，则在phoenix中创建这个表
                    checkTable(sinkTable, tableProcess.getSinkColumns(), tableProcess.getSinkPk(), tableProcess.getSinkExtend());
                }
            }

        }
        if (tableProcessMap==null || tableProcessMap.size() == 0) {
            throw new RuntimeException("缺少处理信息");
        }

    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend){
        //拼接sql语句
        //处理一些特殊情况，如主键为空，扩展字段为空
//        System.out.println("************************进入到checkTable************************");
        if(sinkPk == null){
            sinkPk = "id";
        }
        if(sinkExtend == null){
            sinkExtend = "";
        }

        StringBuffer createTable = new StringBuffer("create table if not exists " + GmallConfig.HBASE_SCHEMA + "." + sinkTable  + "(");

        //拼接字段
        String[] columns = sinkColumns.split(",");
        for(int i = 0;i < columns.length; i++){
            if(sinkPk.equals(columns[i])){
                createTable.append(sinkPk + " varchar primary key,");
            }else{
                if(i != columns.length - 1){
                    createTable.append("info." + columns[i] + " varchar ,");
                }else {
                    createTable.append("info." + columns[i] + " varchar");
                }
            }

        }
        createTable.append(")");
        createTable.append(sinkExtend);
        //打印建表语句
        System.out.println(createTable.toString());

        //执行建表
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(createTable.toString());
            ps.execute();
        } catch (SQLException e) {
            System.out.println("建表失败");
            e.printStackTrace();
        }finally {
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //每一条流数据处理
    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
        String table = value.getString("table");
        String type = value.getString("type");
        JSONObject jsonObj = value.getJSONObject("data");

        //如果是使用Maxwell的初始化功能，那么type类型为bootstrap-insert,我们这里也标记为insert，方便后续处理
        if (type.equals("bootstrap-insert")) {
            type = "insert";
            jsonObj.put("type", type);
        }

        //拼接 key
        String rk = table + "_" + type;
        if(tableProcessMap != null && tableProcessMap.size() > 0){
            //到存储配置的集合中查询是否存在
            TableProcess tableProcess = tableProcessMap.get(rk);
            if(tableProcess != null){
                value.put("sink_table",tableProcess.getSinkTable());
                if (tableProcess.getSinkColumns() != null && tableProcess.getSinkColumns().length() > 0) {
                    //进行ETL处理
                    filterColumn(jsonObj, tableProcess.getSinkColumns());
                }
                //进行分流
                if(tableProcess.getSinkType().equalsIgnoreCase(TableProcess.SINK_TYPE_HBASE)){
                    ctx.output(outputTag,value);
                }else if(tableProcess.getSinkType().equalsIgnoreCase(TableProcess.SINK_TYPE_KAFKA)){
                    out.collect(value);
                }
            }else {
                System.out.println("No This Key:" + rk);
            }
        }
    }

    private void filterColumn(JSONObject data, String sinkColumns) {
        //sinkColumns 表示要保留那些列     id,out_trade_no,order_id
        String[] cols = sinkColumns.split(",");
        //将数组转换为集合，为了判断集合中是否包含某个元素
        List<String> columnList = Arrays.asList(cols);

        //获取json对象中封装的一个个键值对   每个键值对封装为Entry类型
        Set<Map.Entry<String, Object>> entrySet = data.entrySet();

        Iterator<Map.Entry<String, Object>> it = entrySet.iterator();

        for (;it.hasNext();) {
            Map.Entry<String, Object> entry = it.next();
            if(!columnList.contains(entry.getKey())){
                it.remove();
            }
        }
    }
}
