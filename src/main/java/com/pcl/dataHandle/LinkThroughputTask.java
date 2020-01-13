package com.pcl.dataHandle;

import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;


@Service
@EnableScheduling
public class LinkThroughputTask {
        /**
         * Function:没隔3秒统计一次端口吞吐量
         * Author: he
         * Date: 2019/12/23 10:47
         * Notice:
         **/
        @Scheduled(cron = "0/3 * * * * ?")
        public void calLinkThroughput()
        {
            try {
                LinkThroughput.main(new String[] {});
            } catch (Exception e) {
                //TODO: handle exception
            }
            
        }

}
