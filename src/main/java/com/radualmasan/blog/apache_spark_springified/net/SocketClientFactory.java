package com.radualmasan.blog.apache_spark_springified.net;

import org.apache.commons.net.echo.EchoTCPClient;

import java.io.Serializable;

public class SocketClientFactory implements Serializable {

    public EchoTCPClient newSocketClient() {
        return new EchoTCPClient();
    }

}
