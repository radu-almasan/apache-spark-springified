package com.radualmasan.blog.apache_spark_springified;

import com.radualmasan.blog.apache_spark_springified.config.SparkProperties;
import com.radualmasan.blog.apache_spark_springified.function.UncheckedConsumer;
import lombok.RequiredArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.stereotype.Component;

import static org.apache.spark.streaming.Durations.seconds;

@Component
@RequiredArgsConstructor
public class SparkContextManager {

    private final SparkProperties sparkProperties;
    private final SparkConf sparkConf;

    public void withContext(UncheckedConsumer<JavaStreamingContext> consumer) throws Exception {
        try (JavaStreamingContext context = new JavaStreamingContext(sparkConf, seconds(sparkProperties.getWindowSizeInSeconds()))) {
            consumer.accept(context);
        }
    }

}
