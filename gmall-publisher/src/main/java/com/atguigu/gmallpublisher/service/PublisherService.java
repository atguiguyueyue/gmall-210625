package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {

    //日活总数数据接口
    public Integer getDauTotal(String date);

    //日活分时数据接口
    public Map getDauHourTotal(String date);
}
