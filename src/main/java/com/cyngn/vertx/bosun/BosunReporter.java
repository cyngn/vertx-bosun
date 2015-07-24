/*
 * Copyright 2015 Cyanogen Inc.
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
    public static final int DEFAULT_MSG_ERROR_CODE = -1;

    public static final String ACTION_FIELD = "action";
    public static final String PUT_API = "/api/put";
    public static final String INDEX_API = "/api/index";

    public static final int OPENTSDB_DEFAULT_MAX_TAGS = 8;

    private final int DEFAULT_TIMEOUT_MS = 3000;
    private final int DEFAULT_UNIQUE_METRICS_INDEXED = 1000000;
    private final int DEFAULT_INDEX_EXPIRY_MINUTES = 10;
    private static int FIVE_MINUTES_MILLI = 1000 * 60 * 5;

    public final static String RESULT_FIELD = "result";

    private JsonArray hosts;
    private int maxTags;
    private int maxIndexCacheSize;
    private int indexExpiryInMinutes;
    private int timeout;

    private Map<String, Consumer<Message<JsonObject>>> handlers;
    private List<HttpClient> connections;
    private String address;
    private EventBus eventBus;
    private AtomicInteger currentConnectionIndex = new AtomicInteger(0);
    private LoadingCache<String, Boolean> distinctMetrics;
    private long reportingTimerId = -1;
    private AtomicInteger metricsIndexed;
    private AtomicInteger metricsPut;
    private AtomicInteger metricsErrors;

    @Override
    public void start(final Future<Void> startedResult) {

        // setup the default config values
        JsonObject config = context.config();
        hosts = config.getJsonArray("hosts", new JsonArray("[{ \"host\" : \"localhost\", \"port\" : 8070}]"));
        address = config.getString("address", DEFAULT_ADDRESS);
        maxTags = config.getInteger("max_tags", OPENTSDB_DEFAULT_MAX_TAGS);
        maxIndexCacheSize = config.getInteger("max_index_cache_size", DEFAULT_UNIQUE_METRICS_INDEXED);
        indexExpiryInMinutes = config.getInteger("index_expiry_minutes", DEFAULT_INDEX_EXPIRY_MINUTES);
        timeout = config.getInteger("default_timeout_ms", DEFAULT_TIMEOUT_MS);

        metricsIndexed = new AtomicInteger(0);
        metricsPut = new AtomicInteger(0);
        metricsErrors = new AtomicInteger(0);

        eventBus = vertx.eventBus();

        // create the list of workers
        connections = new ArrayList<>(hosts.size());

        initializeConnections(startedResult);
        createMessageHandlers();
        outputConfig();

        // initialize the in memory index cache
        distinctMetrics = CacheBuilder.newBuilder()
        .maximumSize(maxIndexCacheSize)
                .expireAfterWrite(DEFAULT_INDEX_EXPIRY_MINUTES, TimeUnit.MINUTES)
        .build( new CacheLoader<String, Boolean>(){ public Boolean load(String key) throws Exception { return true; }});

        // start listening for incoming messages
        eventBus.consumer(address, this);
        initStatsReporting();
    }


    private void initStatsReporting() {
        reportingTimerId = vertx.setPeriodic(FIVE_MINUTES_MILLI, (timerId) -> {
            logger.info(String.format("Currently indexing %d metrics, metrics indexed: %d put: %d errors: %d this period",
                    distinctMetrics.size(), metricsIndexed.getAndSet(0), metricsPut.getAndSet(0),
                    metricsErrors.getAndSet(0)));
        });
    }


    /**
     * Dump the config that we are using out
     */
    private void outputConfig() {
        StringBuilder builder = new StringBuilder();
        builder.append("Config[address=").append(address).append(", maxTags=").append(maxTags)
               .append(", max_index_cache_size=").append(maxIndexCacheSize).append(", index_expiry_in_minutes=")
               .append(indexExpiryInMinutes).append(", default_timeout_ms=").append(timeout).append(", hosts='")
               .append(hosts.encode()).append("']");
        logger.info(builder.toString());
    }

    /**
     * Setup our client connections
     *
     * @param startedResult the startup callback for loading the module
     */
    private void initializeConnections(Future<Void> startedResult) {
        try {
            for (int i = 0; i < hosts.size(); i++) {
                JsonObject jsonHost = hosts.getJsonObject(i);
                connections.add(vertx.createHttpClient(new HttpClientOptions()
                        .setDefaultHost(jsonHost.getString("host"))
                        .setDefaultPort(jsonHost.getInteger("port"))
                        .setKeepAlive(true)
                        .setTcpNoDelay(true)
                        .setConnectTimeout(timeout)
                        .setTryUseCompression(true)));
            }
        } catch (Exception ex) {
            startedResult.fail(ex.getLocalizedMessage());
            return;
        }
        // all connections added
        startedResult.complete();
    }

    @Override
    public void stop() {
        logger.info("Shutting down vertx-bosun...");
        if (reportingTimerId != -1) {
            vertx.cancelTimer(reportingTimerId);
            reportingTimerId = -1;
        }
    }

    /**
     * Handles round robin'ing through the client connections
     *
     * @return the next client connection to use
     */
    private HttpClient getNextHost() {
       int nextIndex = currentConnectionIndex.incrementAndGet();
       if (nextIndex >= hosts.size()) {
           nextIndex = 0;
           currentConnectionIndex.set(nextIndex);
       }

       return connections.get(nextIndex);
    }

    /**
     * Setup message listeners for specific actions.
     */
    private void createMessageHandlers() {
        handlers = new HashMap<>();
        handlers.put(PUT_COMMAND, this::doPut);
        handlers.put(INDEX_COMMAND, this::doIndex);
    }

    /**
     * Handles posting to the put endpoint
     *
     * @param message the message to send
     */
    private void doPut(Message<JsonObject> message) {
        OpenTsDbMetric metric = getMetricFromMessage(message);
        if(metric == null) { return; }

        metricsPut.incrementAndGet();
        sendData(PUT_API, metric.asJson().encode(), message);
    }

    /**
     * Handles posting to the index endpoint
     *
     * @param message the message to send
     */
    private void doIndex(Message<JsonObject> message) {
        OpenTsDbMetric metric = getMetricFromMessage(message);
        if(metric == null) { return; }

        // ignore it we've seen it lately
        String key = metric.getDistinctKey();
        if (distinctMetrics.getIfPresent(key) != null) {
            message.reply(new JsonObject().put(RESULT_FIELD, BosunResponse.EXISTS_MSG));
            return;
        }

        // cache it
        distinctMetrics.put(key, true);
        metricsIndexed.incrementAndGet();

        sendData(INDEX_API, metric.asJson().encode(), message);
    }

    /**
     * Convert the event bus message to a metric object we can work with.
     *
     * @param message the event bus message
     * @return a metric object that can be sent to Bosun
     */
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
                    maxTags, metric.tags.size()));
            metric = null;
        }
        return metric;
    }

    /**
     * Send data to the bosun instance
     *
     * @param api the api on bosun to send to
     * @param data the json data to send
     * @param message the event bus message the request originated from
     */
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
                message.reply(new JsonObject().put(RESULT_FIELD, BosunResponse.OK_MSG));
            } else {
                response.bodyHandler(responseData -> {
                    sendError(message, "got non 200 response from bosun, error: " + responseData, statusCode);
                });
            }
        })
        .setTimeout(timeout)
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

    /**
     * Send an error message back to the message sender
     *
     * @param message the message to reply to
     * @param error the error text
     * @param errorCode an error code defaults to DEFAULT_MSG_ERROR_CODE, in the case of HTTP failures you'll get a
     *                  status back.
     */
    private void sendError(Message message, String error, int errorCode) {
        metricsErrors.incrementAndGet();
        message.fail(errorCode, error);
    }

    private void sendError(Message message, String error) {
        sendError(message, error, DEFAULT_MSG_ERROR_CODE);
    }
}
