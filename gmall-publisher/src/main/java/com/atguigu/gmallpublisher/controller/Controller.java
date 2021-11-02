package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class Controller {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date) {

        //1.获取总数数据
        Integer dauTotal = publisherService.getDauTotal(date);
        Double gmvTotal = publisherService.getGmvTotal(date);

        //2.创建List集合用来存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        //3.创建存放新增日活的Map集合
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        //创建存放新增设备的Map集合
        HashMap<String, Object> devMap = new HashMap<>();
        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", 233);

        //创建存放新增交易额的Map集合
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", gmvTotal);

        result.add(dauMap);
        result.add(devMap);
        result.add(gmvMap);

        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauHourTotal(@RequestParam("id") String id,
                                  @RequestParam("date") String date
    ) {
        //1.获取Service层处理过后的数据
        //获取昨天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        Map todayMap = null;
        Map yesterdayMap = null;

        //2.创建存放结果数据的Map集合
        if ("dau".equals(id)) {
            todayMap = publisherService.getDauHourTotal(date);
            yesterdayMap = publisherService.getDauHourTotal(yesterday);
        } else if ("order_amount".equals(id)) {
            todayMap = publisherService.getGmvHoutTotal(date);
            yesterdayMap = publisherService.getGmvHoutTotal(yesterday);
        }
        HashMap<String, Map> result = new HashMap<>();

        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        return JSONObject.toJSONString(result);
    }
}
