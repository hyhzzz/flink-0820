package com.atguigu.gmll.service;

import com.atguigu.gmll.bean.ProvinceStats;

import java.util.List;

/**
 * @author chujian
 * @create 2021-05-04 15:07
 */
public interface ProvinceStatsService {
    List<ProvinceStats> getProvinceStats(int date);
}
