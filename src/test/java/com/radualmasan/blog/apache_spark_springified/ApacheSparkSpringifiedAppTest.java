package com.radualmasan.blog.apache_spark_springified;

import com.radualmasan.blog.apache_spark_springified.config.SparkProperties;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.net.Socket;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;
import static org.apache.commons.io.IOUtils.writeLines;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.NONE;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = NONE)
@ActiveProfiles("it")
@Slf4j
public class ApacheSparkSpringifiedAppTest {

    @Rule
    public SocketServer socketServer = new SocketServer();
    @Autowired
    private SparkProperties sparkProperties;

    @Before
    public void setUp() throws IOException {
        socketServer.start(sparkProperties.getInputPort(), sparkProperties.getOutputPort());
    }

    @Test
    public void testJobStarts() throws Exception {
        Socket producerSocket = socketServer.acceptInbound();

        writeLines(Collections.singleton("this foo is a test foo bar"), "\n", producerSocket.getOutputStream());

        List<String> jobOutput = tryToReadFromJobOutput();

        assertThat(jobOutput, is(not(empty())));
        assertThat(jobOutput.get(0), is(equalTo("\"foo\",\"2\"")));
    }

    private List<String> tryToReadFromJobOutput() throws Exception {
        for (int retry = 0; retry < 5; retry++) {
            List<String> strings = socketServer.getConsumerLines();
            if (isNotEmpty(strings)) return strings;

            SECONDS.sleep(sparkProperties.getWindowSizeInSeconds());
        }

        return emptyList();
    }

}