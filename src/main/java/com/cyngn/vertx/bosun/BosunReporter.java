/*
 * Copyright 2014 Cyanogen Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.cyngn.vertx.bosun;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.net.MediaType;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Handles consuming metrics over the message bus and sending them on to bosun.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 07/22/15
 */
public class BosunReporter extends AbstractVerticle implements Handler<Message<JsonObject>> {

    public final static String DEFAULT_ADDRESS = "vertx.bosun-reporter";
    private Logger logger = LoggerFactory.getLogger(BosunReporter.class);
    public static final String PUT_COMMAND = "put";
    public static final String INDEX_COMMAND = "index";

    public static final String ACTION_FIELD = "action";

    public static final String PUT_API = "/api/put";
    public static final String INDEX_API = "/api/index";

    public static final int OPENTSDB_DEFAULT_MAX_TAGS = 8;

    private final int DEFAULT_TIMEOUT = 5000;

    private JsonArray hosts;
    private int maxTags;

    private Map<String, Consumer<Message<JsonObject>>> handlers;
    private List<HttpClient> connections;
    private String address;
    private EventBus eventBus;
    private AtomicInteger currentConnectionIndex = new AtomicInteger(0);
    private LoadingCache<String, Boolean> distinctMetrics;

    @Override
    public void start(final Future<Void> startedResult) {

        JsonObject config = context.config();
        hosts = config.getJsonArray("hosts", new JsonArray("[{ \"host\" : \"localhost\", \"port\" : 8070}]"));
        address = config.getString("address", DEFAULT_ADDRESS);
        maxTags = config.getInteger("maxTags", OPENTSDB_DEFAULT_MAX_TAGS);

        eventBus = vertx.eventBus();

        // create the list of workers
        connections = new ArrayList<>(hosts.size());

        initializeConnections(startedResult);
        createMessageHandlers();
        outputConfig();


        distinctMetrics = CacheBuilder.newBuilder()
        .maximumSize(1000)
        .expireAfterWrite(10, TimeUnit.MINUTES)
        .build(
            new CacheLoader<String, Boolean>() {
                public Boolean load(String key) throws Exception {
                    return true;
                }
        });

        eventBus.consumer(address, this);
    }

    private void outputConfig() {
        StringBuilder builder = new StringBuilder();
        builder.append("Config[address=").append(address).append(", maxTags=").append(maxTags).append(", hosts='")
                .append(hosts.encode()).append("']");
    }

    private void initializeConnections(Future<Void> startedResult) {
        for (int i = 0; i < hosts.size(); i++) {
            JsonObject jsonHost = hosts.getJsonObject(i);
            connections.add(vertx.createHttpClient(new HttpClientOptions()
                    .setDefaultHost(jsonHost.getString("host"))
                    .setDefaultPort(jsonHost.getInteger("port"))
                    .setKeepAlive(true)
                    .setTcpNoDelay(true)
                    .setConnectTimeout(DEFAULT_TIMEOUT)
                    .setTryUseCompression(true)));
        }
        // all connections added
        startedResult.complete();
    }

    @Override
    public void stop() {
        logger.info("Shutting down vertx-bosun...");
    }

    private HttpClient getNextHost() {
       int nextIndex = currentConnectionIndex.incrementAndGet();
       if (nextIndex >= hosts.size()) {
           nextIndex = 0;
           currentConnectionIndex.set(nextIndex);
       }

       return connections.get(nextIndex);
    }

    private void createMessageHandlers() {
        handlers = new HashMap<>();
        handlers.put(PUT_COMMAND, this::doPut);
        handlers.put(INDEX_COMMAND, this::doIndex);
    }

    private void doPut(Message<JsonObject> message) {
        OpenTsDbMetric metric = getMetricFromMessage(message);
        if(metric == null) { return; }

        sendData(PUT_API, metric.asJson().encode(), message);
    }

    private void doIndex(Message<JsonObject> message) {
        OpenTsDbMetric metric = getMetricFromMessage(message);
        if(metric == null) { return; }

        // ignore it we've seen it lately
        String key = metric.getDistinctKey();
        if (distinctMetrics.getIfPresent(key) != null) {
            message.reply(BosunResponse.EXISTS_MSG);
            return;
        }

        // cache it
        distinctMetrics.put(key, true);

        sendData(INDEX_API, metric.asJson().encode(), message);
    }

    private OpenTsDbMetric getMetricFromMessage(Message<JsonObject> message) {
        OpenTsDbMetric metric = null;
        try {
            metric = new OpenTsDbMetric(message.body());
        } catch (IllegalArgumentException ex) {
            sendError(message, ex.getMessage());
            return null;
        }

        if(!metric.validate(maxTags)) {
            sendError(message, String.format("Cannot send more than %d tags, %d were attempted",
                    OPENTSDB_DEFAULT_MAX_TAGS, metric.tags.size()));
            metric = null;
        }
        return metric;
    }

    private void sendData(String api, String data, Message message) {
        HttpClient client = getNextHost();

        Buffer buffer = Buffer.buffer(data.getBytes());

        client.post(api)
        .exceptionHandler(error -> {
            sendError(message, "Got ex contacting bosun, " + error.getLocalizedMessage());
        })
        .handler(response -> {
            int statusCode = response.statusCode();
            // is it 2XX
            if (statusCode >= HttpResponseStatus.OK.code() && statusCode < HttpResponseStatus.MULTIPLE_CHOICES.code()) {
                sendError(message, "got non 200 response from bosun", statusCode);
            } else {
                message.reply(BosunResponse.OK_MSG);
            }
        })
        .setTimeout(DEFAULT_TIMEOUT)
        .putHeader(HttpHeaders.CONTENT_LENGTH, buffer.length() + "")
        .putHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
        .write(buffer)
        .end();
    }

    /**
     * Handles processing metric requests off the event bus
     *
     * @param message the metrics message
     */
    @Override
    public void handle(Message<JsonObject> message) {
        String action = message.body().getString(ACTION_FIELD);

        if (action == null ) { sendError(message, "You must specify an action"); }

        Consumer<Message<JsonObject>> handler = handlers.get(action);

        if ( handler != null) { handler.accept(message); }
        else { sendError(message, "Invalid action: " + action + " specified."); }
    }

    private void sendError(Message message, String error, int errorCode) {
        message.fail(errorCode, error);
    }

    private void sendError(Message message, String error) {
        sendError(message, error, -1);
    }
}
