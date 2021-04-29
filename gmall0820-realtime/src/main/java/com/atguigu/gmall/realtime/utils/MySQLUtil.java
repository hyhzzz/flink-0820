package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.bean.TableProcess;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chujian
 * @create 2021-04-29 15:19
 * 从MySQL数据库中查询数据的工具类
 * ORM：就是将java中的对象和关系型数据库表中的记录建立起映射关系
 * 数据库表和java的类对应
 * 数据库表中的字段和类中的属性对应
 * 数据库中一条记录和java中的对象对应
 */
public class MySQLUtil {

    /**
     * mysql查询方法，根据给定的class类型 返回对应类型的元素列表
     *
     * @param sql               执行的查询语句
     * @param clz               返回的数据类型
     * @param underScoreToCamel 是否将下划线转换为驼峰命名法
     * @param <T>
     * @return
     */
    public static <T> List<T> queryList(String sql, Class<T> clz, Boolean underScoreToCamel) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //1.注册驱动
            Class.forName("com.mysql.jdbc.Driver");
            //2.创建连接
            conn = DriverManager.getConnection(
                    "jdbc:mysql://hadoop102:3306/gmall0820_realtime?characterEncoding=utf-8&useSSL=false",
                    "root",
                    "123456");
            //3.创建数据库操作对象
            ps = conn.prepareStatement(sql);
            //4.执行SQL语句
            rs = ps.executeQuery();
            //5.处理结果集
            //查询结果的元数据信息  可以拿到列的名称
            ResultSetMetaData metaData = rs.getMetaData();
            List<T> resultList = new ArrayList<T>();
            //判断结果集中是否存在数据，如果有那么进行一次循环
            while (rs.next()) {
                //创建一个对象，用于封装查询出来一条结果集中的数据
                T obj = clz.newInstance();
                //对查询的所有列进行遍历，获取每一列的名称
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    String propertyName = "";
                    if (underScoreToCamel) {
                        //如果指定将下划线转换为驼峰命名法的值为true。通过guava工具类，将表中的列转换为类属性驼峰命名法的形式
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    //调用apache的common-bean中工具类 给obj属性赋值
                    BeanUtils.setProperty(obj, propertyName, rs.getObject(i));
                }
                //将当前结果集中的一行数据封装的obj对象放到list集合中
                resultList.add(obj);
            }
            return resultList;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从MySQL查询数据失败");
        } finally {
            //6.释放资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        List<TableProcess> list = queryList("select * from table_process", TableProcess.class, true);
        for (TableProcess tableProcess : list) {
            System.out.println(tableProcess);
        }
    }
}
