package com.atguigu.gmll.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

/**
 * @author chujian
 * @create 2021-05-04 11:44
 * 商品主题统计的Mapper接口
 */
public interface ProductStatsMapper {
    //获取某一天商品的交易额
    @Select("select sum(order_amount) from product_stats_0820 where toYYYYMMDD(stt)=#{date}")
    BigDecimal getGMV(int date);

}
