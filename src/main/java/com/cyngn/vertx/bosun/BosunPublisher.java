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


import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

/**
 * Handles publishing bosun metrics on the event bus.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 11/10/14
 */
public class BosunPublisher {

    private final String address;
    private final EventBus bus;

    public BosunPublisher(String address, EventBus bus) {
        this.address = address;
        this.bus = bus;
    }

    /**
     * Publish an index to bosun to track
     *
     * @param metric the metric name
     * @param value the value
     * @param tags the tags associated
     * @param <T> the type of value int, double etc..
     */
    public <T> void index(String metric, T value, JsonObject tags) {
       index(metric, value, tags, null);
    }

    /**
     * Publish an index to bosun to track
     *
     * @param metric the metric name
     * @param value the value
     * @param tags the tags associated
     * @param onComplete a handler to receive the result of the call
     * @param <T> the type of value int, double etc..
     * @param <U> the type of object coming back in the response
     */
    public <T,U> void index(String metric, T value, JsonObject tags,
                            Handler<AsyncResult<Message<U>>> onComplete) {
        send(getBosunMessage(BosunReporter.INDEX_COMMAND, metric, value, tags), onComplete);
    }

    /**
     * Publish a metric to bosun to be indexed and passed on to OpenTsDb
     *
     * @param metric the metric name
     * @param value the value
     * @param tags the tags associated
     * @param <T> the type of value int, double etc..
     */
    public <T> void put(String metric, T value, JsonObject tags) {
        put(metric, value, tags, null);
    }

    /**
     * Publish a metric to bosun to be indexed and passed on to OpenTsDb
     *
     * @param metric the metric name
     * @param value the value
     * @param tags the tags associated
     * @param onComplete a handler to receive the result of the call
     * @param <T> the type of value int, double etc..
     * @param <U> the type of object coming back in the response
     */
    public <T,U> void put(String metric, T value, JsonObject tags,
                          Handler<AsyncResult<Message<U>>> onComplete) {
        send(getBosunMessage(BosunReporter.PUT_COMMAND, metric, value, tags), onComplete);
    }

    /**
     * Send a metric message over to the vertx-bosun listener
     *
     * @param msg the message to send
     * @param onComplete a handler potentially to pass along
     */
    private <U> void send(JsonObject msg, Handler<AsyncResult<Message<U>>> onComplete) {
        if (onComplete != null) {
            bus.send(address, msg, onComplete);
        } else {
            bus.send(address, msg);
        }
    }

    /**
     * Put the metric data passed in, in the right format for vertx-bosun
     *
     * @param action the action desired, ie index or put
     * @param metric the metric name
     * @param value the value
     * @param tags the tags associated
     * @param <T> the type of value int, double etc..
     * @return the JsonObject representing the metric data
     */
    private static <T> JsonObject getBosunMessage(String action, String metric, T value, JsonObject tags) {
        return  new JsonObject()
                .put(BosunReporter.ACTION_FIELD, action)
                .put(OpenTsDbMetric.METRIC_FIELD, metric)
                .put(OpenTsDbMetric.VALUE_FIELD, value)
                .put(OpenTsDbMetric.TAGS_FIELD, tags);
    }
}
