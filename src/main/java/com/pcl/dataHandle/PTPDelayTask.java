package com.pcl.dataHandle;

import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@EnableScheduling
public class PTPDelayTask {
    /**
     *Function: 每秒一次进行端到端时延计算
     *Author: he
     *Date: 2019/12/19 14:31
     *Notice:
     **/
    @Scheduled(cron = "0/1 * * * * ?")
    public void calPTPDelay()
    {
        try {
            PTPDelay.main(new String[] {});
        } catch (Exception e) {
            //TODO: handle exception
        }
    }
}
