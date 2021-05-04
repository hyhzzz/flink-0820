package com.atguigu.gmll.service;

import java.math.BigDecimal;

/**
 * @author chujian
 * @create 2021-05-04 11:44
 * 商品统计service接口
 */
public interface ProductStatsService {
    //获取某一天交易总额
    BigDecimal getGMV(int date);
}
