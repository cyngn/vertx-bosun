
[![Build Status](https://travis-ci.org/cyngn/vertx-bosun.svg?branch=master)](https://travis-ci.org/cyngn/vertx-bosun)

# vert.x Bosun

This library allows you to send data to a Bosun cluster. Which is an alerting system built around OpenTsDb.

####To use this library you must have a Bosun instance(s) running on your network.

The library creates a long lived HTTP connection to every Bosun endpoint you configure. 

## Name

The library name is `vertx-bosun`

## Configuration

The vertx-bosun module takes the following configuration:

    {
        "address" : <address>,
        "hosts" : [{"host" : <host1>, "port" : <host1Port>}, {"host" : <host2>, "port" : <host2Port>}],
        "max_tags" : <default 8>,
        "max_index_cache_size" : <default 1000000>,
        "index_expiry_minutes" : <default 10>,
        "default_timeout_ms" : <default 3000>
    }

For example:

    {
        "address" : "vertx.bosun-reporter",
        "hosts" : [{"host" : "localhost", "port" : 8070}],
        "max_tags" : 4,
        "max_index_cache_size" : 10000,
        "index_expiry_minutes" : 30,
        "default_timeout_ms" : 1000
    }

Field breakdown:

* `address` The main address for the module. Every module has a main address. Defaults to `vertx.opentsdb-reporter"`.
* `hosts` A list of hosts that represent your Bosun cluster, defaults to a list of one pointing at localhost:8070, in a multiple hosts setup a dedicated http client will be associated per host.
* `max_tags` The max number of tags that the OpenTsdb is configured to handle.  By default, OpenTsdb instances can handle 8, thus we use it as the default here.  If you increase it, make sure all of your OpenTsdb instances have been configured correctly.
* `max_index_cache_size` Bosun indexes open tsdb metrics for tracking, you don't need to send them constantly so we cache them for a time in memory up to a max number of entries, defaults to 1 million entries.
* `index_expiry_minutes` Defines how long before we purge a metric string from the internal cache, defaults to 10 minutes.
* `default_timeout_ms` How long before we fail a request to Bosun, defaults to 3 seconds.

## Operations

### Put

Adds a metric to be sent to OpenTsDb through Bosun, this allows Bosun to index it and to deliver it to OpenTsDb

To add a metric send a JSON message to the module main address:

    {
        "action" : "put",
        "metric" : <metricName>,
        "value" : <metricValue>,
        "tags" : { "key1" : "value1", 
                   "key2" : "value2" 
         }
    }
    
Where: 

* `metric` is the metric name to add to open tsdb and have bosun index, ie 'api.add_item.time'
* `value` the timing data for metric in this example '150.23'
* `tags` : a map of tags to send with just this metric being added, you need at least one tag

An example:

    {
        "action" : "put",
        "metric" : "api.add_item.time",
        "value" : "150.23",
        "tags" : {"type" : "t"}
    }
    
When the add completes successfully (which can be ignored), a reply message is sent back to the sender with the following data:
    
    {
        "result" : "ok"    
    }
    
If an error occurs when adding the metric you will get back a response as failed and you need to check the 'cause' method for the issue, ie

    BosunPublisher publisher = new BosunPublisher("vertx.bosun-reporter", eventBusRef)
     
    JsonObject tags = new JsonObject().put("host", "my.server.com"); 
    publisher.put("my.rest.endpoint.timing", 20.5, tags, AsyncResult<Message<JsonObject>> result -> {
    	if(result.failed()) {
          System.out.println("Got error ex: ", result.cause())
        }
    });          
### Index

Tells Bosun to index a metric but ***DON'T*** send it to OpenTsDb

To index a metric send a JSON message to the module main address:

    {
        "action" : "index",
        "metric" : <metricName>,
        "value" : <metricValue>,
        "tags" : { "key1" : "value1", 
                   "key2" : "value2" 
         }
    }
    
Where: 

* `metric` is the metric name to have bosun index, ie 'api.add_item.time'
* `value` the timing data for metric in this example '150.23'
* `tags` : a map of tags to send with just this metric being added, you need at least one tag

An example:

    {
        "action" : "put",
        "metric" : "api.add_item.time",
        "value" : "150.23",
        "tags" : {"type" : "t"}
    }
    
When the add completes successfully (which can be ignored), a reply message is sent back to the sender with the following data:
    
    {
        "result" : "ok"    
    }
    
If you have already indexed this metric recently and we have it in the cache of metrics already you will receive the following data:    

    {
        "result" : "exists"    
    }
    
If an error occurs when adding the metric you will get back a response as failed and you need to check the 'cause' method for the issue, ie

    BosunPublisher publisher = new BosunPublisher("vertx.bosun-reporter", eventBusRef)
     
    JsonObject tags = new JsonObject().put("host", "my.server.com"); 
    publisher.index("my.rest.endpoint.timing", 20.5, tags, AsyncResult<Message<JsonObject>> result -> {
    	if(result.failed()) {
          System.out.println("Got error ex: ", result.cause())
        }
    });      

#### Example code 
You can send the messages to the library directly via the event bus but it's easiest just to use the provided BosunPublisher class as follows:

     BosunPublisher publisher = new BosunPublisher("vertx.bosun-reporter", eventBusRef)
     
     JsonObject tags = new JsonObject().put("host", "my.server.com"); 
     publisher.index("my.rest.endpoint.timing", 20.5, tags);
     publisher.put("my.other.rest.endpoint.timing", 31.5, tags);
     
     publisher.index("my.rest.endpoint.timing", 20.5, tags, (AsyncResult<Message<JsonObject>> response) -> {
            System.out.println(response.result().body());
     });     
     