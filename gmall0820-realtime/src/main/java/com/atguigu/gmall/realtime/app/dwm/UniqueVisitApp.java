package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author chujian
 * @create 2021-05-01 0:10
 * 独立访客UV计算
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {

        //TODO 1.基本环境准备
        //1.1 准备流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //主要本地有一个ui界面
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 设置检查点
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //设置超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(6000);
//        env.setStateBackend( new FsStateBackend("hdfs://hadoop102:8020/gmall/flink/checkpoint/uniquevisit"));

        //TODO 2.从kafka dwd_page_log中读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "unique_Visit_App_group";
        String sinkTopic = "dwm_unique_visit";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> jsonStrDS = env.addSource(kafkaSource);

        //TODO 3.对读取到的数据进行结构转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr));

//        jsonObj.print(">>>");

        //TODO 4.按照设备id进行分组
        KeyedStream<JSONObject, String> keybyWithMidDS = jsonObjDS.keyBy(
                jsonObj -> jsonObj.getJSONObject("common").getString("mid")
        );


        //TODO 5.过滤得到UV
        SingleOutputStreamOperator<JSONObject> filteredDS = keybyWithMidDS.filter(
                new RichFilterFunction<JSONObject>() {
                    //定义状态
                    ValueState<String> lastVisitDateState = null;
                    //定义日期工具类
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化日期工具类
                        sdf = new SimpleDateFormat("yyyyMMdd");
                        //初始化状态
                        ValueStateDescriptor<String> lastVisitDateStateDS = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        //因为统计的是当日UV，也就是日活，所以为状态设置失效时间
                        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();
                        lastVisitDateStateDS.enableTimeToLive(stateTtlConfig);
                        this.lastVisitDateState = getRuntimeContext().getState(lastVisitDateStateDS);
                    }

                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        //首先判断当前页面是否从别的页面跳转过来的
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId != null && lastPageId.length() > 0) {
                            return false;
                        }
                        //获取当前访问时间
                        Long ts = jsonObj.getLong("ts");
                        //将当前访问时间戳转换为日期字符串
                        String logDate = sdf.format(new Date(ts));
                        //获取状态日期
                        String lastVisitDate = lastVisitDateState.value();

                        //用当前页面的访问时间和状态实际进行对象
                        if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(logDate)) {
                            System.out.println("已访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
                            return false;
                        } else {
                            System.out.println("未访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
                            lastVisitDateState.update(logDate);
                            return true;
                        }
                    }
                }
        );

//        filteredDS.print(">>>>>");


        //TODO 6.向kafka中写回，需要将json转换为string
        //6.1 json -> string
        SingleOutputStreamOperator<String> kafkaDS = filteredDS.map(jsonObj -> jsonObj.toJSONString());

        //6.2 写回到kafka的dwm层
        kafkaDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));


        //执行任务
        env.execute();
    }
}
