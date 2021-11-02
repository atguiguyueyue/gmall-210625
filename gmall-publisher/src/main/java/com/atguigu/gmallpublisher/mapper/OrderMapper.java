package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {

    //获取当天总数GMV
    public Double selectOrderAmountTotal(String date);

    //获取当天分时数据
    public List<Map> selectOrderAmountHourMap(String date);
}
