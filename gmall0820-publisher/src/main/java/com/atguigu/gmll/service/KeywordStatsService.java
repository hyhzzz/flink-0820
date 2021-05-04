package com.atguigu.gmll.service;

import com.atguigu.gmll.bean.KeywordStats;

import java.util.List;

/**
 * @author chujian
 * @create 2021-05-04 15:41
 * 关键词统计接口
 */
public interface KeywordStatsService {
    public List<KeywordStats> getKeywordStats(int date, int limit);
}
