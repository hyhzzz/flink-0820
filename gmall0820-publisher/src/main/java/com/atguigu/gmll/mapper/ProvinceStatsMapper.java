package com.atguigu.gmll.mapper;

import com.atguigu.gmll.bean.ProvinceStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author chujian
 * @create 2021-05-04 15:06
 * 按照地区统计交易额
 */
public interface ProvinceStatsMapper {
    @Select("select province_name,sum(order_amount) order_amount from province_stats_0820 " +
            "where toYYYYMMDD(stt)=#{date} group by province_id,province_name")
    List<ProvinceStats> selectProvinceStats(int date);
}
