package com.atguigu.gmall.realtime.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author chujian
 * @create 2021-04-29 18:32
 * 配置表处理函数
 */
public class TableProcessFunction extends ProcessFunction<JSONObject, JSONObject> {
    //因为要将维度数据写到侧输出流，所以定义一个侧输出流标签
    private OutputTag<JSONObject> outputTag;

    public TableProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {


    }

    @Override
    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> collector) throws Exception {

    }
}
