package com.radualmasan.blog.apache_spark_springified.config;

import org.apache.spark.SparkConf;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(SparkProperties.class)
public class SparkConfig {

    @Bean
    public SparkConf sparkConf(SparkProperties sparkProperties) {
        return new SparkConf()
                .setMaster(sparkProperties.getMaster())
                .setAppName(sparkProperties.getAppName());
    }

}
