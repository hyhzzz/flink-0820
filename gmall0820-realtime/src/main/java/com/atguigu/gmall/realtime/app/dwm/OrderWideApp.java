package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.OrderDetail;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @author chujian
 * @create 2021-05-01 19:21
 * 处理订单和订单明细数据形成订单宽表
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {

        //TODO 1.基本环境准备
        //1.1 准备流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 设置检查点
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //设置超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(6000);
//        env.setStateBackend( new FsStateBackend("hdfs://hadoop102:8020/gmall/flink/checkpoint/uniquevisit"));


        //TODO 2. 从kafka的DWD层来读取订单和订单明细数据
        //2.1声明相关的主题以及消费者组
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        //2.2读取订单主题数据
        FlinkKafkaConsumer<String> orderInfoSource = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        DataStreamSource<String> orderInfoJsonDS = env.addSource(orderInfoSource);

        //2.3读取订单明细数据
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);
        DataStreamSource<String> orderDetailJsonStrDS = env.addSource(orderDetailSource);

        //TODO 3.对读取的数据进行结构的转换  jsonstring -> orderinfo|orderdetail
        //3.1转换订单数据结构
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoJsonDS.map(
                new RichMapFunction<String, OrderInfo>() {
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderInfo map(String jsonStr) throws Exception {
                        OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                        //format把日期转换为字符串
                        //parse把字符串转换为日期
                        orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                        return orderInfo;
                    }
                }
        );

        //3.2 转换订单明细数据结构
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailJsonStrDS.map(
                new RichMapFunction<String, OrderDetail>() {
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderDetail map(String jsonStr) throws Exception {
                        OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                        orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                        return orderDetail;
                    }
                }
        );

//        orderInfoDS.print("orderInfo>>>");
//        orderDetailDS.print("orderDetail>>>");

        //TODO 4.指定事件时间字段
        //4.1 订单指定事件时间字段
        SingleOutputStreamOperator<OrderInfo> orderInfoWithTsDS = orderInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(
                        Duration.ofSeconds(3)

                )
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderInfo>() {
                                    @Override
                                    public long extractTimestamp(OrderInfo orderInfo, long l) {
                                        return orderInfo.getCreate_ts();
                                    }
                                }
                        )
        );

        //4.2 订单明细指定事件时间字段
        SingleOutputStreamOperator<OrderDetail> orderDetailWithTsDS = orderDetailDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(
                        Duration.ofSeconds(3)
                )
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderDetail>() {
                                    @Override
                                    public long extractTimestamp(OrderDetail orderDetail, long l) {
                                        return orderDetail.getCreate_ts();
                                    }
                                }
                        )
        );

        //TODO 5.按照订单id进行分组  指定关联的key
        KeyedStream<OrderInfo, Long> orderInfoKeyedDS = orderInfoWithTsDS.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailKeyedDS = orderDetailWithTsDS.keyBy(OrderDetail::getOrder_id);

        //TODO 6.使用intervalJoin对订单和订单明细进行关联
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeyedDS
                .intervalJoin(orderDetailKeyedDS)
                .between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(
                        new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                            @Override
                            public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                                out.collect(new OrderWide(orderInfo, orderDetail));
                            }
                        }
                );

//        orderWideDS.print("orderWide>>>>");



        //TODO 7.



        //执行任务
        env.execute();
    }
}