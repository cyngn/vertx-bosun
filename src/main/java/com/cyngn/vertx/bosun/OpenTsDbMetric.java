package com.cyngn.vertx.bosun;

import io.vertx.core.json.JsonObject;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Object representing OpenTsDb metrics.
 *
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
        if(obj == null) { throw new IllegalArgumentException("You must supply a non-null JsonObject"); }

        this.metric = obj.getString(METRIC_FIELD);
        this.value = obj.getValue(VALUE_FIELD);
        this.tags = obj.getJsonObject(TAGS_FIELD);
        timestamp = System.currentTimeMillis();
        validateObj();
    }

    /**
     * Constructor
     *
     * @param metric the metric name
     * @param value the metric value
     * @param tags any tags associated to the metric
     */
    public OpenTsDbMetric(String metric, Object value, JsonObject tags) {
        this.metric = metric;
        this.value = value;
        this.tags = tags;
        timestamp = System.currentTimeMillis();
        validateObj();
    }

    /**
     * Is the object valid?
     */
    private void validateObj() {
        if(StringUtils.isEmpty(metric)) { throw new IllegalArgumentException("All metrics need a 'name' field"); }
        if(value == null) { throw new IllegalArgumentException("All metrics need a 'value' field");  }

        if(tags == null || tags.size() == 0) {
            throw new IllegalArgumentException("You must specify at least one tag");
        }
    }

    /**
     * Get the object as a vertx JsonOBject
     *
     * @return the metric data in Json form
     */
    public JsonObject asJson() {
        JsonObject obj = new JsonObject().put(METRIC_FIELD, metric).put(VALUE_FIELD, value)
                .put(TIMESTAMP_FIELD, timestamp);

        JsonObject tagsObj = new JsonObject();
        for(String key : tags.fieldNames()){
            tagsObj.put(key, tags.getValue(key));
        }

        // there has to be at least one tag
        obj.put(TAGS_FIELD, tagsObj);
        return obj;
    }

    /**
     * Get a deterministic representation of the metric name including the tags
     *
     * @return a unique key to represent this metric
     */
    public String getDistinctKey() {
        StringBuilder builder = new StringBuilder();
        builder.append(metric);

        // these always need to be in the same order to make comparing work
        List<String> keys = new ArrayList<>(tags.fieldNames());
        Collections.sort(keys);

        for (String tag : keys) {
            builder.append("::").append(tag).append(":").append(tags.getString(tag));
        }

        return builder.toString();
    }

    /**
     * Validate that the object is valid
     *
     * @param maxTags the max number of tags that can be used
     * @return true if valid false otherwise
     */
    public boolean validate(int maxTags) {
        return tags.size() <= maxTags;
    }

}
