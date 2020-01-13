package com.pcl.dataHandle;

import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@EnableScheduling
public class DelayJitterTask {

    /**
     *Function: 每2秒一次进行时延抖动计算
     *Author: he
     *Date: 2019/12/19 14:29
     *Notice:
     **/
    @Scheduled(cron = "0/2 * * * * ?")
    public void calJitter()
    {
        try {
            DelayJitter.main(new String[] {});
        } catch (Exception e) {
            //TODO: handle exception
        }
        
    }
}
