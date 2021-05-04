package com.atguigu.gmll.controller;

import com.atguigu.gmll.bean.ProductStats;
import com.atguigu.gmll.bean.ProvinceStats;
import com.atguigu.gmll.bean.VisitorStats;
import com.atguigu.gmll.service.ProductStatsService;
import com.atguigu.gmll.service.ProvinceStatsService;
import com.atguigu.gmll.service.VisitorStatsService;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.*;

/**
 * @author chujian
 * @create 2021-05-04 11:45
 * <p>
 * 大屏展示的控制层
 * <p>
 * 主要职责：接收客户端的请求(request)，对请求进行处理，并给客户端响应(response)
 * @RestController = @Controller + @ResponseBody
 * @RequestMapping()可以加在类和方法上 加在类上，就相当于指定了访问路径的命名空间
 */
@RestController
@RequestMapping("/api/sugar")
public class SugarController {

    //将service注入进来
    @Autowired
    ProductStatsService productStatsService;

    @Autowired
    ProvinceStatsService provinceStatsService;

    @Autowired
    VisitorStatsService visitorStatsService;


    /*
    请求路径
    $API_HOST/api/sugar/visitor
     */
    @RequestMapping("/visitor")
    public Map getVisitorStatsByNewFlag(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = now();
        }

        //调用service层，获取访客统计相关指标数据
        List<VisitorStats> visitorStatsByNewFlagList = visitorStatsService.getVisitorStatsByNewFlag(date);

        //定义两个对象，分别接收新老访客统计的结果
        VisitorStats newVisitorStats = new VisitorStats();
        VisitorStats oldVisitorStats = new VisitorStats();

        //对查询的数据进行遍历，给新老访客统计对象赋值
        for (VisitorStats visitorStats : visitorStatsByNewFlagList) {
            if ("1".equals(visitorStats.getIs_new())) {
                newVisitorStats = visitorStats;
            } else {
                oldVisitorStats = visitorStats;
            }
        }

        //返回的json字符串的处理
        Map resMap = new HashMap();
        resMap.put("status", 0);
        Map dataMap = new HashMap();
        dataMap.put("combineNum", 1);

        //表头
        List columnList = new ArrayList();
        Map typeHeader = new HashMap();
        typeHeader.put("name", "类别");
        typeHeader.put("id", "type");
        columnList.add(typeHeader);

        Map newHeader = new HashMap();
        newHeader.put("name", "新用户");
        newHeader.put("id", "new");
        columnList.add(newHeader);

        Map oldHeader = new HashMap();
        oldHeader.put("name", "老用户");
        oldHeader.put("id", "old");
        columnList.add(oldHeader);
        dataMap.put("columns", columnList);

        //表格bady
        List rowList = new ArrayList();
        //用户数
        Map userCount = new HashMap();
        userCount.put("type", "用户数(人)");
        userCount.put("new", newVisitorStats.getUv_ct());
        userCount.put("old", oldVisitorStats.getUv_ct());
        rowList.add(userCount);

        //总访问页面
        Map pageTotal = new HashMap();
        pageTotal.put("type", "总访问页面(次)");
        pageTotal.put("new", newVisitorStats.getPv_ct());
        pageTotal.put("old", oldVisitorStats.getPv_ct());
        rowList.add(pageTotal);

        //跳出率
        Map jumRate = new HashMap();
        jumRate.put("type", "跳出率(%)");
        jumRate.put("new", newVisitorStats.getUjRate());
        jumRate.put("old", oldVisitorStats.getUjRate());
        rowList.add(jumRate);

        //平均在线时长
        Map ageDurTime = new HashMap();
        ageDurTime.put("type", "平均在线时长(秒)");
        ageDurTime.put("new", newVisitorStats.getDurPerSv());
        ageDurTime.put("old", oldVisitorStats.getDurPerSv());
        rowList.add(ageDurTime);

        //平均页面访问人数
        Map ageVisitCount = new HashMap();
        ageVisitCount.put("type", "平均访问人数(人次)");
        ageVisitCount.put("new", newVisitorStats.getPvPerSv());
        ageVisitCount.put("old", oldVisitorStats.getPvPerSv());
        rowList.add(ageVisitCount);

        dataMap.put("rows", rowList);
        resMap.put("data", dataMap);
        return resMap;
    }

    /**
     * 请求路径
     * $API_HOST/api/sugar/province
     */
    @RequestMapping("/province")
    public String getProvinceStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = now();
        }
        //从service中获取地区统计数据
        List<ProvinceStats> provinceStatsList = provinceStatsService.getProvinceStats(date);
        StringBuilder jsonBuilder = new StringBuilder("{\"status\": 0,\"data\": {\"mapData\": [");

        for (int i = 0; i < provinceStatsList.size(); i++) {
            ProvinceStats provinceStats = provinceStatsList.get(i);
            if (i >= 1) {
                jsonBuilder.append(",");
            }
            jsonBuilder.append("{\"name\": \"" + provinceStats.getProvince_name() + "\",\"value\": " + provinceStats.getOrder_amount() + "}");
        }
        jsonBuilder.append("]}}");
        return jsonBuilder.toString();
    }

    /**
     * 请求路径
     * $API_HOST/api/sugar/spu?limit=10
     */
    @RequestMapping("/spu")
    public String getProductStatsBySPU(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "10") Integer limit
    ) {
        if (date == 0) {
            date = now();
        }
        //调用service层方法，获取按spu统计数据
        List<ProductStats> productStatsBySPUList = productStatsService.getProductStatsBySPU(date, limit);
        //初始化表头信息
        StringBuilder jsonBuilder = new StringBuilder("{" +
                "\"status\": 0," +
                "\"data\": {" +
                "\"columns\": [{" +
                "\"name\": \"商品SPU名称\"," +
                "\"id\": \"spu_name\"" +
                "}," +
                "{" +
                "\"name\": \"交易额\"," +
                "\"id\": \"order_amount\"" +
                "}," +
                "{" +
                "\"name\": \"订单数\"," +
                "\"id\": \"order_ct\"" +
                "}" +
                "]," +
                "\"rows\": [");
        //对查询出来的数据进行遍历，将每一条遍历的结果封装为json的一行数据
        for (int i = 0; i < productStatsBySPUList.size(); i++) {
            ProductStats productStats = productStatsBySPUList.get(i);
            if (i >= 1) {
                jsonBuilder.append(",");
            }
            jsonBuilder.append("{" +
                    "\"spu_name\": \"" + productStats.getSpu_name() + "\"," +
                    "\"order_amount\":" + productStats.getOrder_amount() + "," +
                    "\"order_ct\":" + productStats.getOrder_ct() + "}"
            );
        }

        jsonBuilder.append("]}}");
        return jsonBuilder.toString();
    }


    /**
     * 处理请求的路径
     * $API_HOST/api/sugar/category3?limit=5
     */
    @RequestMapping("/category3")
    public Map getProductStatsByCategory3(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "10") Integer limit
    ) {
        if (date == 0) {
            date = now();
        }
        //调用service获取品类交易额排行
        List<ProductStats> productStatsByCategory3List = productStatsService.getProductStatsByCategory3(date, limit);
        Map resMap = new HashMap();
        resMap.put("status", 0);
        List dataList = new ArrayList();
        for (ProductStats productStats : productStatsByCategory3List) {
            Map dataMap = new HashMap();
            dataMap.put("name", productStats.getCategory3_name());
            dataMap.put("value", productStats.getOrder_amount());
            dataList.add(dataMap);
        }

        resMap.put("data", dataList);
        return resMap;
    }

    /*
       -请求地址
       $API_HOST/api/sugar/trademark?limit=5
    /*
 方式1：使用字符串拼接的方式处理返回的json数据
    @RequestMapping("/trademark")
    public String getProductStatsByTrademark(
        @RequestParam(value = "date", defaultValue = "0") Integer date,
        @RequestParam(value = "limit", defaultValue = "10") Integer limit) {

        //如果没有传递日期参数，那么将日期设置为当前日期
        if (date == 0) {
            date = now();
        }
        //调用service根据品牌获取交易额排名
        List<ProductStats> productStatsByTrademarkList = productStatsService.getProductStatsByTrademark(date, limit);

        //定义两个集合，分别存放品牌的名称以及品牌的交易额
        List<String> trademarkNameList = new ArrayList<>();
        List<BigDecimal> amountList = new ArrayList<>();

        //对获取到的品牌交易额进行遍历
        for (ProductStats productStats : productStatsByTrademarkList) {
            trademarkNameList.add(productStats.getTm_name());
            amountList.add(productStats.getOrder_amount());

        }
        String json = "{" +
            "\"status\": 0," +
            "\"data\": {" +
            "\"categories\": [\"" + StringUtils.join(trademarkNameList, "\",\"") + "\"]," +
            "\"series\": [" +
            "{" +
            "\"data\": [" + StringUtils.join(amountList, ",") + "]" +
            "}]}}";

        return json;
    }*/

    //方式2：封装对象，通过将对象转换的json格式字符串的方式 返回json数据
    @RequestMapping("/trademark")
    public Map getProductStatsByTrademark(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "10") Integer limit) {

        //如果没有传递日期参数，那么将日期设置为当前日期
        if (date == 0) {
            date = now();
        }
        //调用service根据品牌获取交易额排名
        List<ProductStats> productStatsByTrademarkList = productStatsService.getProductStatsByTrademark(date, limit);

        //定义两个集合，分别存放品牌的名称以及品牌的交易额
        List<String> trademarkNameList = new ArrayList<>();
        List<BigDecimal> amountList = new ArrayList<>();

        //对获取到的品牌交易额进行遍历
        for (ProductStats productStats : productStatsByTrademarkList) {
            trademarkNameList.add(productStats.getTm_name());
            amountList.add(productStats.getOrder_amount());

        }
        Map resMap = new HashMap();
        resMap.put("status", 0);
        Map dataMap = new HashMap();
        dataMap.put("categories", trademarkNameList);
        List seriesList = new ArrayList();
        Map seriesDataMap = new HashMap();
        seriesDataMap.put("data", amountList);
        seriesList.add(seriesDataMap);
        dataMap.put("series", seriesList);
        resMap.put("data", dataMap);
        return resMap;
    }

    /**
     * 请求路径： /api/sugar/gmv
     */
    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = now();
        }
        BigDecimal gmv = productStatsService.getGMV(date);
        String json = "{" +
                "\"status\": 0," +
                "\"data\": " + gmv +
                "}";
        return json;
    }

    private Integer now() {
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }
}
