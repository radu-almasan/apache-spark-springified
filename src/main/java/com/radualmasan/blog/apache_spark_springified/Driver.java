package com.radualmasan.blog.apache_spark_springified;

import com.radualmasan.blog.apache_spark_springified.config.SparkProperties;
import com.radualmasan.blog.apache_spark_springified.net.SocketClientFactory;
import lombok.RequiredArgsConstructor;
import org.apache.commons.net.echo.EchoTCPClient;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.apache.commons.io.IOUtils.writeLines;

@Component
@RequiredArgsConstructor
public class Driver implements Serializable {

    private final SparkProperties sparkProperties;
    private final SocketClientFactory jobOutputSocketClientFactory;
    private final transient SparkContextManager sparkContextManager;

    public void run() throws Exception {
        sparkContextManager.withContext(context -> {
            setUpPipeLine(context);

            context.start();
            context.awaitTermination();
        });
    }

    private void setUpPipeLine(JavaStreamingContext context) {
        context.socketTextStream(sparkProperties.getInputHost(), sparkProperties.getInputPort())
                .mapPartitionsToPair(this::splitIntoWords)
                .reduceByKey(Integer::sum)
                .foreachRDD(this::writeToOutputSocket);
    }

    private Iterator<Tuple2<String, Integer>> splitIntoWords(Iterator<String> iterator) {
        return stream(spliteratorUnknownSize(iterator, 0), false)
                .map(line -> line.split("\\s"))
                .flatMap(words -> Arrays.stream(words).map(word -> new Tuple2<>(word, 1)))
                .iterator();
    }

    private void writeToOutputSocket(JavaPairRDD<String, Integer> rdd) throws IOException {
        EchoTCPClient client = jobOutputSocketClientFactory.newSocketClient();
        try {
            client.connect(sparkProperties.getOutputHost(), sparkProperties.getOutputPort());

            List<String> lines = rdd.mapToPair(pair -> new Tuple2<>(pair._2, pair._1))
                    .sortByKey(false, 1)
                    .map(tuple -> String.format("\"%s\",\"%d\"", tuple._2, tuple._1))
                    .collect();

            writeLines(lines, "\n", client.getOutputStream());

        } finally {
            if (client.isConnected())
                client.disconnect();
        }
    }

}
