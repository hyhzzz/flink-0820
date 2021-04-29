package com.atguigu.gmall.realtime.common;

/**
 * @author chujian
 * @create 2021-04-29 21:15
 * 项目配置的常量类
 */
public class GmallConfig {
    //Hbase的命名空间
    public static final String HBASE_SCHEMA = "GMALL0820_REALTIME";

    //Phonenix连接的服务器地址
    public static final String PHOENIX_SERVER="jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

}
