package com.atguigu.gmall.realtime.bean;

/**
 * @author chujian
 * @create 2021-04-29 14:58
 * 配置表对应的实体类
 */
//@Data
public class TableProcess {
    //动态分流Sink常量 改为小写和脚本一致
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";
    //来源表
    String sourceTable;
    //操作类型 insert,update,delete
    String operateType;
    //输出类型 hbase kafka
    String sinkType;
    //输出表(主题)
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;

    public TableProcess() {
    }

    public TableProcess(String sourceTable, String operateType, String sinkType, String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        this.sourceTable = sourceTable;
        this.operateType = operateType;
        this.sinkType = sinkType;
        this.sinkTable = sinkTable;
        this.sinkColumns = sinkColumns;
        this.sinkPk = sinkPk;
        this.sinkExtend = sinkExtend;
    }

    public static String getSinkTypeHbase() {
        return SINK_TYPE_HBASE;
    }

    public static String getSinkTypeKafka() {
        return SINK_TYPE_KAFKA;
    }

    public static String getSinkTypeCk() {
        return SINK_TYPE_CK;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getOperateType() {
        return operateType;
    }

    public void setOperateType(String operateType) {
        this.operateType = operateType;
    }

    public String getSinkType() {
        return sinkType;
    }

    public void setSinkType(String sinkType) {
        this.sinkType = sinkType;
    }

    public String getSinkTable() {
        return sinkTable;
    }

    public void setSinkTable(String sinkTable) {
        this.sinkTable = sinkTable;
    }

    public String getSinkColumns() {
        return sinkColumns;
    }

    public void setSinkColumns(String sinkColumns) {
        this.sinkColumns = sinkColumns;
    }

    public String getSinkPk() {
        return sinkPk;
    }

    public void setSinkPk(String sinkPk) {
        this.sinkPk = sinkPk;
    }

    public String getSinkExtend() {
        return sinkExtend;
    }

    public void setSinkExtend(String sinkExtend) {
        this.sinkExtend = sinkExtend;
    }

    @Override
    public String toString() {
        return "TableProcess{" +
                "sourceTable='" + sourceTable + '\'' +
                ", operateType='" + operateType + '\'' +
                ", sinkType='" + sinkType + '\'' +
                ", sinkTable='" + sinkTable + '\'' +
                ", sinkColumns='" + sinkColumns + '\'' +
                ", sinkPk='" + sinkPk + '\'' +
                ", sinkExtend='" + sinkExtend + '\'' +
                '}';
    }
}