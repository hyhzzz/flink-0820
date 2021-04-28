package com.atguigu.gmall;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

//springboot执行的一个入口类
//当springboot执行的时候，会扫描同级别包以及子包下的所有类，交给spring进行管理
@SpringBootApplication
public class Gmall0820LoggerApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall0820LoggerApplication.class, args);
    }

}
