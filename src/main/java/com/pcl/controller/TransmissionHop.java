package com.pcl.controller;

import com.alibaba.fastjson.JSONObject;
import com.pcl.service.AnalysisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Program: TransmissionHop
 * @Author: Liuzhentao
 * @Description: 计算传输跳数接口
 * @Date: 9:47 2019/12/3
 */
@RestController
public class TransmissionHop {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransmissionHop.class);

    @Autowired
    private AnalysisService analysisService;

    @RequestMapping("transmissionHop")
    public JSONObject receiveMsg(@RequestBody JSONObject body){

        return analysisService.getTransmissionHop(body);
    }
}
