package com.cyngn.vertx.bosun;

import io.vertx.core.json.JsonObject;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author truelove@cyngn.com (Jeremy Truelove) 7/21/15
 */
public class OpenTsDbMetric {

    public static String METRIC_FIELD = "metric";
    public static String VALUE_FIELD = "value";
    public static String TAGS_FIELD = "tags";
    public static String TIMESTAMP_FIELD = "timestamp";

    public final String metric;
    public final Object value;
    public final long timestamp;
    public final JsonObject tags;

    public OpenTsDbMetric(JsonObject obj) {
        this(obj.getString(METRIC_FIELD), obj.getValue(VALUE_FIELD), obj.getJsonObject(TAGS_FIELD));
    }

    public OpenTsDbMetric(String metric, Object value, JsonObject tags) {
        this.metric = metric;
        this.value = value;
        this.tags = tags;
        timestamp = System.currentTimeMillis();

        if(StringUtils.isEmpty(metric)) { throw new IllegalArgumentException("All metrics need a 'name' field"); }
        if(value == null) { throw new IllegalArgumentException("All metrics need a 'value' field");  }

        if(tags == null || tags.size() == 0) {
            throw new IllegalArgumentException("You must specify at least one tag");
        }
    }

    public JsonObject asJson() {
        JsonObject obj = new JsonObject().put(METRIC_FIELD, metric).put(VALUE_FIELD, value).put(TIMESTAMP_FIELD, timestamp);

        JsonObject tagsObj = new JsonObject();
        for(String key : tags.fieldNames()){
            tagsObj.put(key, tags.getValue(key));
        }

        // there has to be at least one tag
        obj.put(TAGS_FIELD, tagsObj);
        return obj;
    }

    public String getDistinctKey() {
        StringBuilder builder = new StringBuilder();
        builder.append(metric);

        // these always need to be in the same order to make comparing work
        List<String> keys = new ArrayList<>(tags.fieldNames());
        Collections.sort(keys);

        for (String tag : keys) {
            builder.append("-").append(tag);
        }

        return builder.toString();
    }

    public boolean validate(int maxTags) {
        return tags.size() <= maxTags;
    }

}
