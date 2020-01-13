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
 * @Program: End2EndDelay
 * @Author: Liuzhentao
 * @Description: 计算端到端时延，时延抖动接口
 * @Date: 9:47 2019/12/3
 */
@RestController
public class End2EndDelay {
    private static final Logger LOGGER = LoggerFactory.getLogger(End2EndDelay.class);

    @Autowired
    private AnalysisService analysisService;

    @RequestMapping("end2EndDelay")
    public JSONObject receiveMsg(@RequestBody JSONObject body){

        return analysisService.getEnd2EndDelay(body);
    }
}
