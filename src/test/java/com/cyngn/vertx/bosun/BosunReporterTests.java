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

import io.vertx.core.*;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.impl.Deployment;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Example Java integration test (You need to have bosun running)
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 11/10/14
 */
//@Ignore("Integration tests, comment out annotation to run the tests")
@RunWith(VertxUnitRunner.class)
public class BosunReporterTests {

    private Vertx vertx;
    private EventBus eb;
    private static String topic = "test-opentsdb";
    private BosunPublisher publisher;

    @Before
    public void before(TestContext context) {
        vertx = Vertx.vertx();
        eb = vertx.eventBus();

        JsonObject config = new JsonObject();
        config.put("address", topic);
        JsonArray array = new JsonArray();
        array.add(new JsonObject().put("host", "bosun-docker.dev-boxy.cyanogen.net").put("port", 80));
        //array.add(new JsonObject().put("host", "localhost").put("port", 8070));
        config.put("hosts", array);
        config.put("max_tags", 1);

        Async async = context.async();
        vertx.deployVerticle(BosunReporter.class.getName(), new DeploymentOptions().setConfig(config),
                result -> {
                    if (!result.succeeded()) {
                        result.cause().printStackTrace();
                        context.fail(result.cause());
                    }
                    async.complete();
                });

        publisher = new BosunPublisher(topic, eb);
    }

    @After
    public void after(TestContext context) {
        Deployment deployment = ((VertxInternal) vertx).getDeployment(vertx.deploymentIDs().iterator().next());
        vertx.undeploy(deployment.deploymentID());
    }


    @Test
    public void testInvalidAction(TestContext context) throws Exception {
        JsonObject metric = new JsonObject();

        Async async = context.async();
        eb.send(topic, metric, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> result) {
                context.assertTrue(result.failed());
                context.assertTrue(result.cause() instanceof ReplyException);
                context.assertEquals(result.cause().getMessage(), "You must specify an action");
                async.complete();
            }
        });

        Async async1 = context.async();
        metric = new JsonObject().put("action", "badCommand");
        eb.send(topic, metric, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> result) {
                context.assertTrue(result.failed());
                context.assertTrue(result.cause() instanceof ReplyException);
                context.assertEquals(result.cause().getMessage(), "Invalid action: badCommand specified.");
                async1.complete();
            }
        });
    }

    @Test
    public void testNoTags(TestContext context) throws Exception {
        JsonObject metric = new JsonObject();
        metric.put("action", BosunReporter.PUT_COMMAND);
        metric.put("metric", "test.value");
        metric.put("value", "34.4");
        Async async = context.async();
        eb.send(topic, metric, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> result) {
                context.assertTrue(result.failed());
                context.assertTrue(result.cause() instanceof ReplyException);
                context.assertEquals(result.cause().getMessage(), "You must specify at least one tag");
                async.complete();
            }
        });
    }

    @Test
    public void testNoName(TestContext context) throws Exception {
        JsonObject metric = new JsonObject();
        metric.put("action", BosunReporter.PUT_COMMAND);
        metric.put("value", "34.4");
        Async async = context.async();
        eb.send(topic, metric, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> result) {
                context.assertTrue(result.failed());
                context.assertTrue(result.cause() instanceof ReplyException);
                context.assertEquals(result.cause().getMessage(), "All metrics need a 'name' field");
                async.complete();
            }
        });
    }

    @Test
    public void testNoValue(TestContext context) throws Exception {
        JsonObject metric = new JsonObject();
        metric.put("action", BosunReporter.PUT_COMMAND);
        metric.put("metric", "test.metric");
        Async async = context.async();
        eb.send(topic, metric, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> result) {
                context.assertTrue(result.failed());
                context.assertTrue(result.cause() instanceof ReplyException);
                context.assertEquals(result.cause().getMessage(), "All metrics need a 'value' field");
                async.complete();
            }
        });
    }

    @Test
    public void testSend(TestContext context) throws Exception {
        Async async = context.async();
        publisher.index("vertx.bosun.test", 50, new JsonObject().put("foo", "bar"),
                (AsyncResult<Message<JsonObject>> event) -> {
                    if (event.failed()) {
                        context.fail();
                        return;
                    }

                    context.assertEquals(BosunResponse.OK_MSG, event.result().body().getString(BosunReporter.RESULT_FIELD));

                    publisher.index("vertx.bosun.test", 70, new JsonObject().put("foo", "bar"),
                            (AsyncResult<Message<JsonObject>> event2) -> {
                                if (event2.failed()) {
                                    context.fail();
                                    return;
                                }

                                context.assertEquals(BosunResponse.EXISTS_MSG,
                                        event2.result().body().getString(BosunReporter.RESULT_FIELD));
                                async.complete();
                            });
                });
    }

    @Test
    public void testSendMany(TestContext context) throws Exception {
        JsonObject metric = new JsonObject();
        metric.put("action", BosunReporter.INDEX_COMMAND);
        metric.put("metric", "test.value");
        metric.put("value", "34.4");
        metric.put("tags", new JsonObject().put("foo", "bar"));

        int totalMessages = 10000;
        AtomicInteger count = new AtomicInteger(0);
        Async async = context.async();

        AtomicInteger okCount = new AtomicInteger(0);

        Handler<AsyncResult<Message<JsonObject>>> handler = result -> {
            if (result.failed()) { context.fail(); }

            String response = result.result().body().getString(BosunReporter.RESULT_FIELD);
            if (StringUtils.equals(BosunResponse.OK_MSG, response)) {
                okCount.incrementAndGet();
            }
            else if (StringUtils.equals(BosunResponse.EXISTS_MSG, response)) {
            } else { context.fail(); }

            if(count.incrementAndGet() == totalMessages) {
                if (okCount.get() != 1) {
                    context.fail();
                    return;
                }
                async.complete();
            }
        };

        for(int i = 0; i < totalMessages; i++) { eb.send(topic, metric, new DeliveryOptions(), handler); }
    }

    @Test
    public void testTooManyTags(TestContext context) throws Exception {
        JsonObject metric = new JsonObject();
        metric.put("action", BosunReporter.PUT_COMMAND);
        metric.put("metric", "test.value");
        metric.put("value", "34.4");
        metric.put("tags",
                new JsonObject().put("foo", "bar")
                        .put("var", "val"));
        Async async = context.async();
        eb.send(topic, metric, new DeliveryOptions(), new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> result) {
                context.assertTrue(result.failed());
                context.assertTrue(result.cause() instanceof ReplyException);
                context.assertEquals(result.cause().getMessage(), "Cannot send more than 1 tags, 2 were attempted");
                async.complete();
            }
        });
    }
}
