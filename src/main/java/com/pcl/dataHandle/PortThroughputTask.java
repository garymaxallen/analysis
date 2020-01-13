package com.pcl.dataHandle;

import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@EnableScheduling
public class PortThroughputTask {
    /**
     * Function:没隔3秒统计一次端口吞吐量
     * Author: he
     * Date: 2019/12/23 10:47
     * Notice:
     **/
    @Scheduled(cron = "0/3 * * * * ?")
    public void calPortThroughput()
    {
        try {
            PortThroughput.main(new String[] {});
        } catch (Exception e) {
            //TODO: handle exception
        }
        
    }
}
