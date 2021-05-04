package com.atguigu.gmll.service.impl;

import com.atguigu.gmll.bean.VisitorStats;
import com.atguigu.gmll.mapper.VisitorStatsMapper;
import com.atguigu.gmll.service.VisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author chujian
 * @create 2021-05-04 15:11
 * 访客统计接口的实现类
 */
@Service
public class VisitorStatsServiceImpl implements VisitorStatsService {

    @Autowired
    VisitorStatsMapper visitorStatsMapper;

    @Override
    public List<VisitorStats> getVisitorStatsByNewFlag(int date) {
        return visitorStatsMapper.selectVisitorStatsByNewFlag(date);
    }

    @Override
    public List<VisitorStats> getVisitorStatsByHr(int date) {
        return visitorStatsMapper.selectVisitorStatsByHr(date);
    }
}
