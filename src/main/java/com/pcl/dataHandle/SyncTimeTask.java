package com.pcl.dataHandle;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@EnableScheduling
public class SyncTimeTask {

    @Scheduled(cron = "0/2 * * * * ?")
    public void calSyncTime()
    {
        try {
            SyncTime.main(new String[] {});
        } catch (Exception e) {
            //TODO: handle exception
        }
    }
}
