package com.atguigu.gmll.service.impl;

import com.atguigu.gmll.bean.KeywordStats;
import com.atguigu.gmll.mapper.KeywordStatsMapper;
import com.atguigu.gmll.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author chujian
 * @create 2021-05-04 15:41
 * 关键词统计接口实现类
 */
@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {
    @Autowired
    KeywordStatsMapper keywordStatsMapper;

    @Override
    public List<KeywordStats> getKeywordStats(int date, int limit) {
        return keywordStatsMapper.selectKeywordStats(date,limit);
    }
}
