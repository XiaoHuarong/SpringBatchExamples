package com.flower;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringBatchExampleApplication {

    private  static Logger log = LoggerFactory.getLogger(SpringBatchExampleApplication.class);
    public static void main(String[] args) {
        SpringApplication.run(SpringBatchExampleApplication.class, args);
        log.info("###############  Start Success!  ###############");
    }

}

