package com.atguigu.gmall.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author chujian
 * @create 2021-04-28 15:13
 * 日志处理服务
 */
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("/applog")
    public String getLogger(@RequestParam("param") String jsonLog) {

        //1.打印输出到控制台
//        System.out.println(jsonLog);

        //2.将数据落盘  可以借助记录日志的第三方框架  log4j logback
        log.info(jsonLog);

        //3.将生成的日志数据发送至Kafka ODS主题
        kafkaTemplate.send("ods_base_log", jsonLog);

        return "success";
    }
}
