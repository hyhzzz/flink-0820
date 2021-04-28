package com.atguigu.gmall.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author chujian
 * @create 2021-04-28 15:05
 */
//标识为controller组件，交给Sprint容器管理，并接收处理请求  如果返回String，会当作网页进行跳转
//RestController = @Controller + @ResponseBody  会将返回结果转换为json进行响应
@RestController
public class FirstController {

    //处理客户端的请求，并且进行响应
    @RequestMapping("/test")
    public String test(){

        return "success";
    }
}
