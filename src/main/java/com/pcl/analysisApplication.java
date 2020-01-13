package com.pcl;

// import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * @Program: analysisApplication
 * @Author: Liuzhentao
 * @Description:
 * @Date: 17:11 2019/12/2
 */

@SpringBootApplication
@EnableAsync
public class analysisApplication {
    public static void main(String[] args) {
        SpringApplication.run(analysisApplication.class);
    }
}
