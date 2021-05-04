package com.atguigu.gmll.service.impl;

import com.atguigu.gmll.bean.ProvinceStats;
import com.atguigu.gmll.mapper.ProvinceStatsMapper;
import com.atguigu.gmll.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author chujian
 * @create 2021-05-04 15:07
 */
@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {
    //注入mapper
    @Autowired
    ProvinceStatsMapper provinceStatsMapper;

    @Override
    public List<ProvinceStats> getProvinceStats(int date) {
        return provinceStatsMapper.selectProvinceStats(date);
    }
}
