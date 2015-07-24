package com.cyngn.vertx.bosun;

import io.vertx.core.json.JsonObject;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 * @author truelove@cyngn.com (Jeremy Truelove) 7/24/15
 */
public class OpenTsDbMetricTest {

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidName(){
        new OpenTsDbMetric(null, null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidValue(){
        new OpenTsDbMetric("foo", null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTags(){
        new OpenTsDbMetric("foo", 5, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidJsonObject(){
        new OpenTsDbMetric(null);
    }

    @Test
    public void testDistinctKey() {
        OpenTsDbMetric metric = new OpenTsDbMetric("test.metric", 5, new JsonObject().put("host", "my.host.com"));
        assertEquals("test.metric-host:my.host.com", metric.getDistinctKey());

        // make sure tag ordering doesn't matter
        metric = new OpenTsDbMetric("test.metric", 5, new JsonObject().put("host", "my.host.com").put("asset", "2"));
        assertEquals("test.metric-asset:2-host:my.host.com", metric.getDistinctKey());

        metric = new OpenTsDbMetric("test.metric", 5, new JsonObject().put("asset", "2").put("host", "my.host.com"));
        assertEquals("test.metric-asset:2-host:my.host.com", metric.getDistinctKey());
    }
}
