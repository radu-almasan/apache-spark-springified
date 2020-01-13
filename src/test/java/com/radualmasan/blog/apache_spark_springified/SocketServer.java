package com.radualmasan.blog.apache_spark_springified;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.DefaultSocketFactory;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.io.IOUtils.readLines;

@Slf4j
public class SocketServer extends ExternalResource {

    private DefaultSocketFactory socketFactory = new DefaultSocketFactory();
    private ServerSocket producerServerSocket;
    private ServerSocket consumerServerSocket;

    public void start(Integer inputPort, Integer outputPort) throws IOException {
        producerServerSocket = socketFactory.createServerSocket(inputPort);
        consumerServerSocket = socketFactory.createServerSocket(outputPort);
    }

    public Socket acceptInbound() throws Exception {
        return acceptAndGet(producerServerSocket);
    }

    public List<String> getConsumerLines() throws Exception {
        return supplyAsync(() -> {
            try {
                Socket socket = consumerServerSocket.accept();
                return readLines(socket.getInputStream());

            } catch (Exception e) {
                LOG.error("Error while waiting for output", e);
                return null;
            }
        }).get(10, SECONDS);
    }

    @Override
    protected void after() {
        try {
            if (consumerServerSocket != null && !consumerServerSocket.isClosed())
                consumerServerSocket.close();
        } catch (IOException e) {
            LOG.error("Problems closing consumer server socket", e);
        }

        try {
            if (producerServerSocket != null && !producerServerSocket.isClosed())
                producerServerSocket.close();
        } catch (IOException e) {
            LOG.error("Problems closing producer server socket", e);
        }
    }

    private Socket acceptAndGet(ServerSocket socket) throws Exception {
        return supplyAsync(() -> {
            try {
                return socket.accept();

            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }).get(10, SECONDS);
    }
}
