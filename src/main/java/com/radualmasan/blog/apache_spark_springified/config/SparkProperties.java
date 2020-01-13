package com.radualmasan.blog.apache_spark_springified.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.io.Serializable;

@ConfigurationProperties("spark")
@Data
public class SparkProperties implements Serializable {

    private String master;
    private String appName;
    private long windowSizeInSeconds;
    protected String inputHost;
    protected Integer inputPort;
    protected String outputHost;
    protected Integer outputPort;

}
