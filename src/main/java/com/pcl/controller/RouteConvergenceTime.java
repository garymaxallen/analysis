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
 * @Program: RouteConvergenceTime
 * @Author: Liuzhentao
 * @Description: 计算路由收敛时间接口
 * @Date: 9:48 2019/12/3
 */
@RestController
public class RouteConvergenceTime {
    private static final Logger LOGGER = LoggerFactory.getLogger(RouteConvergenceTime.class);

    @Autowired
    private AnalysisService analysisService;

    @RequestMapping("routeConvergenceTime")
    public JSONObject receiveMsg(@RequestBody JSONObject body){

        return analysisService.getRouteConvergenceTime(body);
    }
}
