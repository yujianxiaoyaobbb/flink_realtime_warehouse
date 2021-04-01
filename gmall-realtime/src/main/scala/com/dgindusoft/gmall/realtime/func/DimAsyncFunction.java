package com.dgindusoft.gmall.realtime.func;

import com.alibaba.fastjson.JSONObject;
import com.dgindusoft.gmall.realtime.utils.DimUtil;
import com.dgindusoft.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    //线程池对象
    private ExecutorService es;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    /**
     * 初始化线程池对象
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("初始化线程池对象");
        es = ThreadPoolUtil.getInstance();
    }

    /**
     * 每一个线程的执行逻辑
     *
     * @param obj
     * @param resultFuture
     * @throws Exception
     */
    @Override
    public void  asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        es.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    long start = System.currentTimeMillis();
                    String key = getKey(obj);
                    JSONObject dimInfo = DimUtil.getDimInfo(tableName, key);
                    if (dimInfo != null && dimInfo.size() > 0) {
                        join(obj,dimInfo);
                    }
                    long end = System.currentTimeMillis();
//                    System.out.println("总计用时;" + (end - start) + "ms");
                    //将关联后的数据继续向下传递
                    resultFuture.complete(Arrays.asList(obj));
                }catch (Exception e){
                    throw new RuntimeException("维度查询失败");
                }
            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("超时了：" + input);
    }
}
