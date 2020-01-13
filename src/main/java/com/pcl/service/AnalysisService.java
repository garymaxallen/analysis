package com.pcl.service;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * @Program: analysisService
 * @Author: Liuzhentao
 * @Description: 分析服务
 * @Date: 20:27 2019/12/4
 */
@Service
public class AnalysisService {
    private static final Logger LOGGER = LoggerFactory.getLogger(AnalysisService.class);

    /**
    * @describe: 获取端到端时延
    * @author: liuzhentao
    * @date: 2019/12/4 20:32
    * @param: [dataJson]
    * @return: com.alibaba.fastjson.JSONObject
    */
    public JSONObject getEnd2EndDelay(JSONObject dataJson)
    {
        return null;
    }

    /**
    * @describe: 获取吞吐量
    * @author: liuzhentao
    * @date: 2019/12/4 20:32
    * @param: [dataJson]
    * @return: com.alibaba.fastjson.JSONObject
    */
    public JSONObject getHandingCapacity(JSONObject dataJson)
    {
        return null;
    }

    /**
    * @describe:  获取丢包率
    * @author: liuzhentao
    * @date: 2019/12/4 20:33
    * @param: [dataJson]
    * @return: com.alibaba.fastjson.JSONObject
    */
    public JSONObject getPacketLoss(JSONObject dataJson)
    {
        return null;
    }

    /**
    * @describe: 获取路由收敛时间
    * @author: liuzhentao
    * @date: 2019/12/4 20:34
    * @param: [dataJson]
    * @return: com.alibaba.fastjson.JSONObject
    */
    public JSONObject getRouteConvergenceTime(JSONObject dataJson)
    {
        return null;
    }

    /**
    * @describe: 获取传输跳数
    * @author: liuzhentao
    * @date: 2019/12/4 20:37
    * @param: [dataJson]
    * @return: com.alibaba.fastjson.JSONObject
    */
    public JSONObject getTransmissionHop(JSONObject dataJson)
    {
        return null;
    }
}
