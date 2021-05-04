package com.atguigu.gmll.service;

import com.atguigu.gmll.bean.VisitorStats;

import java.util.List;

/**
 * @author chujian
 * @create 2021-05-04 15:11
 */
public interface VisitorStatsService {
    List<VisitorStats> getVisitorStatsByNewFlag(int date);

    List<VisitorStats> getVisitorStatsByHr(int date);
}
