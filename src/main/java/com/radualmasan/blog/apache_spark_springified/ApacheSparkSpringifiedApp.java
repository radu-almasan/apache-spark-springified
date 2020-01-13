package com.radualmasan.blog.apache_spark_springified;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import static java.util.concurrent.TimeUnit.SECONDS;

@SpringBootApplication
@RequiredArgsConstructor
public class ApacheSparkSpringifiedApp {

    private final Driver driver;

    public static void main(String[] args) throws InterruptedException {
        ConfigurableApplicationContext context = SpringApplication.run(ApacheSparkSpringifiedApp.class);
        blockMainThreadUntilSparkFinishes(context);
    }

    private static void blockMainThreadUntilSparkFinishes(ConfigurableApplicationContext context) throws InterruptedException {
        ThreadPoolTaskExecutor executor = context.getBean(ThreadPoolTaskExecutor.class);
        while (executor.getActiveCount() > 0)
            SECONDS.sleep(5);
        executor.shutdown();
    }

    @Async
    @EventListener(ApplicationStartedEvent.class)
    public void onApplicationStarted() throws Exception {
        driver.run();
    }

}
