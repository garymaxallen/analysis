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
 * @Program: HandlingCapacity
 * @Author: Liuzhentao
 * @Description: 计算吞吐量接口
 * @Date: 9:49 2019/12/3
 */
@RestController
public class HandlingCapacity {
    private static final Logger LOGGER = LoggerFactory.getLogger(HandlingCapacity.class);

    @Autowired
    private AnalysisService analysisService;

    @RequestMapping("handlingCapacity")
    public JSONObject receiveMsg(@RequestBody JSONObject body){

        return analysisService.getHandingCapacity(body);
    }
}
