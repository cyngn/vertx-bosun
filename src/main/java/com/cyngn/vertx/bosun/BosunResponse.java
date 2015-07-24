package com.cyngn.vertx.bosun;

/**
 * Event bus responses.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 7/23/15
 */
public interface BosunResponse {

    /**
     * Everything went fine with your message.
     */
    String OK_MSG = "ok";
    /**
     * You passed in a metric that is still under the threshold of caching limits and being cached
     */
    String EXISTS_MSG = "exists";
}
