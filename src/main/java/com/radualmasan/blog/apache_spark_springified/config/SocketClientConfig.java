package com.radualmasan.blog.apache_spark_springified.config;

import com.radualmasan.blog.apache_spark_springified.net.SocketClientFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SocketClientConfig {

    @Bean
    public SocketClientFactory jobOutputSocketClientSupplier() {
        return new SocketClientFactory();
    }

}
