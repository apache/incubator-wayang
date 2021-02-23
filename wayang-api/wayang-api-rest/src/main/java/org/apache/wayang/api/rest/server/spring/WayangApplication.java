package org.apache.wayang.api.rest.server.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WayangApplication {

    public static void main(String[] args) {
        SpringApplication.run(WayangApplication.class, args);
    }
}
