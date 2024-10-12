package io.javaoperatorsdk.operator.sample;

import com.sun.net.httpserver.HttpServer;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.sample.probes.LivenessHandler;
import io.javaoperatorsdk.operator.sample.probes.StartupHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class RedisOperator {
    private static final Logger log = LoggerFactory.getLogger(RedisOperator.class);

    public static void main(String[] args) throws IOException {
        log.info("Redis Operator starting!");

        Operator operator = new Operator();
        operator.register(new RedisReconciler());
        operator.start();

        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        server.createContext("/startup", new StartupHandler(operator));
        server.createContext("/healthz", new LivenessHandler(operator));
        server.setExecutor(null);
        server.start();
    }
}