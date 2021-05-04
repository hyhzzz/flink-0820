package com.atguigu.gmall.realtime.bean;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author chujian
 * @create 2021-05-03 22:39
 * 用该注解标记的属性，不需要插入到ClickHouse
 * 向ClickHouse写入数据的时候，如果有字段数据不需要传输，可以用该注解标记
 */
@Target(FIELD)
@Retention(RUNTIME)
public @interface TransientSink {
}
