package io.javaoperatorsdk.operator.sample.probes;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.javaoperatorsdk.operator.Operator;

import java.io.IOException;

import static io.javaoperatorsdk.operator.sample.probes.StartupHandler.sendMessage;
import static java.util.Objects.requireNonNull;

public class LivenessHandler implements HttpHandler {
    private final Operator operator;

    public LivenessHandler(Operator operator) {
        this.operator = requireNonNull(operator);
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        if (operator.getRuntimeInfo().allEventSourcesAreHealthy()) {
            sendMessage(httpExchange, 200, "healthy");
        } else {
            sendMessage(httpExchange, 400, "an event source is not healthy");
        }
    }
}
