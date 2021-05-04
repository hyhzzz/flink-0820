package com.atguigu.gmll.service.impl;

import com.atguigu.gmll.mapper.ProductStatsMapper;
import com.atguigu.gmll.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * @author chujian
 * @create 2021-05-04 11:44
 */
@Service //标识是Spring的Sevice层组件，将对象的创建交给Spring的IOC管理
public class ProductStatsServiceImpl implements ProductStatsService {

    //自动注入   在容器中，寻找ProductStatsMapper类型的对象，赋值给当前属性
    @Autowired
    ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGMV(int date) {
        return productStatsMapper.getGMV(date);
    }

}
