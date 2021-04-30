package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.func.DimSink;
import com.atguigu.gmall.realtime.func.TableProcessFunction;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author chujian
 * @create 2021-04-29 14:08
 * 准备业务数据的DWD
 * 从Kafka中读取ods层业务数据 并进行处理  动态分流发送到不同的topic中(DWD层)
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {

        //TODO 1.准备环境
        //1.1创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度  和kafka分区保持一致
        env.setParallelism(4);

        //1.3 为了保证精准一次性 需要设置checkpoint检查点 并且设置相关的参数 默认就是EXACTLY_ONCE
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //1.4设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //1.5设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/flink/checkpoint/basedbApp"));

        //重启策略  如果说没有开启Checkpoint 那么重启策略就是noRestart 表示不重启
        //重启策略  如果说开启Checkpoint 那么重启策略会产生自动帮你重启  重启integer = maxvalue
        //env.setRestartStrategy(RestartStrategies.noRestart());

        //TODO 2.从kafka的ods层读取数据
        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";

        //2.1通过工具类获取kafka的消费者
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> JsonStrDS = env.addSource(kafkaSource);

//        JsonStrDS.print("json>>>>>");

        //TODO 3.对DS中数据进行结构转换  string -> json
//        JsonStrDS.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> jsonObjDS = JsonStrDS.map(
                jsonStr -> JSON.parseObject(jsonStr)
        );
        //TODO 4.对数据进行ETL清洗,如果table为空，或者data为空，或者data长度<3，将这样的数据过滤掉
        SingleOutputStreamOperator<JSONObject> filteredDS = jsonObjDS.filter(
                jsonObj -> {
                    boolean flag = jsonObj.getString("table") != null
                            && jsonObj.getJSONObject("data") != null
                            && jsonObj.getString("data").length() >= 3;

                    return flag;
                }
        );

//        filteredDS.print("json>>>>");

        //TODO 5. 动态分流  事实表放到主流，写回到kafka的DWD层；如果维度表，通过侧输出流，写入到Hbase
        //5.1定义输出到Hbase的侧输出流标签
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };

        //5.2 主流 写回到Kafka的数据
        SingleOutputStreamOperator<JSONObject> kafkaDS = filteredDS.process(
                new TableProcessFunction(hbaseTag)
        );
        //5.3获取侧输出流    写到Hbase的数据
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);

        kafkaDS.print("事实>>>>");
        hbaseDS.print("维度>>>>");

        //TODO 6.将维度数据保存到Phoenix对应的维度表中
        hbaseDS.addSink(new DimSink());

        //TODO 7.将事实数据写回到kafka中的WDW层
        FlinkKafkaProducer<JSONObject> kafkaSink = MyKafkaUtil.getKafkaSinkBySchema(
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public void open(SerializationSchema.InitializationContext context) throws Exception {
                        System.out.println("kafka序列化");
                    }

                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long aLong) {
                        String sinkTopic = jsonObj.getString("sink_table");
                        JSONObject dataJsonObj = jsonObj.getJSONObject("data");
                        return new ProducerRecord<>(sinkTopic, dataJsonObj.toString().getBytes());
                    }
                }
        );

        kafkaDS.addSink(kafkaSink);


        //执行任务
        env.execute();
    }
}
