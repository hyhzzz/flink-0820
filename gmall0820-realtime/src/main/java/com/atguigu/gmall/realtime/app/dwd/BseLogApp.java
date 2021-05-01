package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author chujian
 * @create 2021-04-28 23:17
 * 从Kafka中读取ods层用户行为日志数据
 */
public class BseLogApp {

    //定义要发送的主题
    private static final String TOPIC_START = "dwd_start_log";
    private static final String TOPIC_DISPLAYS = "dwd_displays_log";
    private static final String TOPIC_PAGE = "dwd_page_log";

    public static void main(String[] args) throws Exception {

        //TODO 1.创建环境
        //1.1创建Flink流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2设置并行度 ，和kafka分区保持一致
        env.setParallelism(4);

        //1.3设置checkpoint检查点
        //开启checkpoint 多久执行一次checkpoint,默认就是精准一次
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //设置checkpoint超时时间  超过多长时间就会被废弃
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/flink/checkpoint/baselogApp"));

        //注意 ：这里读取用户的时候读取的是当前操作系统的用户，操作hdfs的时候使用的win用户所以不认识
        //方式一解决 ：hdfs dfs -chmod -R 777 /   设置权限
        //方式二解决：
        //System.setProperty("HADOOP_USER_NAME","atguigu");

        //TODO 2.从kafka中读取数据
        String topic = "ods_base_log";
        String groupId = "base_log_app_group";

        //2.1调用kafka工具类，获取FlinkKafkaConSumer
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //TODO 3.对读取到的数据格式进行转换  string -> jsonObject对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        return jsonObject;
                    }
                }
        );

//        jsonObjDS.print("json>>>>>");

        //TODO 4.识别新来访客
        // 前端也会对新老状态进行记录，有可能会不准，我们这里只是做一次确认
        // 将相同的mid放到一组 保存mid当前某天访问情况（将首次访问日期作为状态保存起来），等后面该设备在有日志过来的时候，从状态中获取日期
        // 和当前日志的产生日期进行对比，如果状态不为空，并且状态日期和当前日志日期不相等，说明是老访客，如果is_new标记的是1，
        // 那么对其状态进行修复

        //4.1 根据mid对日志进行分组
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(
                data -> data.getJSONObject("common").getString("mid")
        );

        //4.2新老访客状态修复 状态分为算子状态和键控状态， 我们这要记录某一个设备的访问，使用键控状态比较合适
        SingleOutputStreamOperator<JSONObject> jsonDSWithFlag = midKeyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    //定义该mid的访问状态
                    private ValueState<String> firstVisitDateState;
                    //定义日期格式化对象
                    private SimpleDateFormat sdf;

                    //初始化声明周期
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //对状态以及日期格式进行初始化
                        firstVisitDateState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("newMidDateState", String.class)
                        );
                        sdf = new SimpleDateFormat("yyyyMMdd");
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        //获取当前日志标记的状态
                        String is_New = jsonObj.getJSONObject("common").getString("is_new");

                        //获取当前日访问志时间戳
                        Long ts = jsonObj.getLong("ts");

                        if ("1".equals(is_New)) {
                            //获取当前mid对应的状态
                            String stateDate = firstVisitDateState.value();
                            //对当前这条日志的日期格式进行转换
                            String curDate = sdf.format(new Date(ts));

                            //如果状态不为空，并且状态日期和当前日志日期不相等，说明是老访客
                            if (stateDate != null && stateDate.length() != 0) {
                                //判断是否为同一天数据
                                if (!stateDate.equals(curDate)) {
                                    is_New = "0";
                                    jsonObj.getJSONObject("common").put("is_new", is_New);
                                }
                            } else {
                                //如果还没记录设备的状态，将当前访问日志作为状态值
                                firstVisitDateState.update(curDate);
                            }
                        }
                        return jsonObj;
                    }
                }
        );

//        jsonDSWithFlag.print(">>>>>>>>>>");

        //TODO 5.分流操作
        // 根据日志数据内容,将日志数据分为3类, 页面日志、启动日志和曝光日志。
        // 页面日志输出到主流,启动日志输出到启动侧输出流,曝光日志输出到曝光日志侧输出流
        // 侧输出流：1.接收迟到数据  2.分流

        //定义启动侧输出标签
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        //定义曝光侧输出流标签
        OutputTag<String> displaysTag = new OutputTag<String>("displays") {
        };

        SingleOutputStreamOperator<String> pageDS = jsonDSWithFlag.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, Context context, Collector<String> collector) throws Exception {
                        //获取启动日志标记
                        JSONObject startJsonObj = jsonObj.getJSONObject("start");

                        //将json格式转换为字符串，方便向侧输出流输出以及向kakfa中写入
                        String dataStr = jsonObj.toString();

                        //判断是否为启动日志
                        if (startJsonObj != null && startJsonObj.size() > 0) {
                            //如果是启动日志 输出到启动侧输出流中
                            context.output(startTag, dataStr);
                        } else {
                            //如果不是启动日志，说明是页面 日志 输出到主流
                            collector.collect(dataStr);

                            //如果是不是启动日志，获取曝光日志标签（曝光日志中也携带了页面日志）
                            JSONArray displays = jsonObj.getJSONArray("displays");
                            //判断是否为曝光日志
                            if (displays != null && displays.size() > 0) {
                                //如果是曝光日志,遍历输出到侧输出流
                                for (int i = 0; i < displays.size(); i++) {
                                    //获取每一条曝光事件
                                    JSONObject displaysJsonObj = displays.getJSONObject(i);
                                    //获取页面id
                                    String pageId = jsonObj.getJSONObject("page").getString("page_id");
                                    //给每一条曝光事件加pageId
                                    displaysJsonObj.put("page_id", pageId);
                                    //转成字符串输出
                                    context.output(displaysTag, displaysJsonObj.toString());
                                }
                            }
                        }
                    }
                }
        );

        //获取侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displaysTag);

        //打印输出
        pageDS.print("page>>>>");
        startDS.print("start>>>");
        displayDS.print("display>>>");


        //TODO 6.将不同流的数据写回到kafka的不同topic中 （DWD层）
        FlinkKafkaProducer<String> startSink = MyKafkaUtil.getKafkaSink(TOPIC_START);
        startDS.addSink(startSink);

        FlinkKafkaProducer<String> displaysSink = MyKafkaUtil.getKafkaSink(TOPIC_DISPLAYS);
        displayDS.addSink(displaysSink);

        FlinkKafkaProducer<String> pageSink = MyKafkaUtil.getKafkaSink(TOPIC_PAGE);
        pageDS.addSink(pageSink);

        //执行任务
        env.execute();

    }
}
