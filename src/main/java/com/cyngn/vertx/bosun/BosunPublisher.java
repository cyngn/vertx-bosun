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

    public <T> void indexMetric(String metric, T value, JsonObject tags) {
       indexMetric(metric, value, tags, null);
    }

    public <T> void indexMetric(String metric, T value, JsonObject tags,
                                Handler<AsyncResult<Message<Object>>> onComplete) {
        sendMessage(getBosunMessage(BosunReporter.INDEX_COMMAND, metric, value, tags), onComplete);
    }

    public <T> void putMetric(String metric, T value, JsonObject tags) {
        putMetric(metric, value, tags, null);
    }

    public <T> void putMetric(String metric, T value, JsonObject tags,
                              Handler<AsyncResult<Message<Object>>> onComplete) {
        sendMessage(getBosunMessage(BosunReporter.PUT_COMMAND, metric, value, tags), onComplete);
    }

    private void sendMessage(JsonObject msg, Handler<AsyncResult<Message<Object>>> onComplete) {
        if (onComplete != null) {
            bus.send(address, msg, onComplete);
        } else {
            bus.send(address, msg);
        }
    }

    private static <T> JsonObject getBosunMessage(String action, String metric, T value, JsonObject tags) {
        return  new JsonObject()
                .put(BosunReporter.ACTION_FIELD, action)
                .put(OpenTsDbMetric.METRIC_FIELD, metric)
                .put(OpenTsDbMetric.VALUE_FIELD, value)
                .put(OpenTsDbMetric.TAGS_FIELD, tags);
    }
}
