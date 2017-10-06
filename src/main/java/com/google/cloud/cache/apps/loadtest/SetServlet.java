package com.google.cloud.cache.apps.loadtest;

import com.google.appengine.api.memcache.MemcacheSerialization;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheService.SetPolicy;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.common.collect.Range;
import com.google.common.io.BaseEncoding;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.IOException;
import java.util.logging.Logger;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Cache a random value for a serializable java object used as key.
 *
 * This handler demonstrates mapping human readable key to actual encoded key stored in Memcache,
 * the encoded key is same as encoded by App Engine Memcache API.
 *
 * This handler also demonatrates logging such keys in RPC failures so that the key can be
 * correlated to external monitoring source such as keys returned in hotkey API.
 */
public final class SetServlet extends HttpServlet {
  private static final Logger logger = Logger.getLogger(SetServlet.class.getName());

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    RequestReader reader = RequestReader.create(request);
    ResponseWriter writer = ResponseWriter.create(response);
    MemcacheService memcache = MemcacheServiceFactory.getMemcacheService();

    Range<Integer> valueSizeRange = reader.readValueSizeRange();
    int iterationCount = reader.readIterationCount();
    String key = reader.readKey();
    for (int i = 0; i < iterationCount; ++i) {
      //java.io.Serializable key = new java.util.Date();
      String encodedKey = getEncodedKey(key);
      writer.write(String.format("key=%s, encodedKey=%s\n", key, encodedKey));
      String value = MemcacheValues.random(valueSizeRange);
      try {
        boolean result = memcache.put(key, value, null, SetPolicy.SET_ALWAYS);
        // This is intenally to test logging of the key, reverse the logic when logging on failures.
        if (result) {
          logger.info(String.format("Log put failure for key=%s, encodedKey=%s\n", key, encodedKey));
        }
      } catch (Exception e) {
        logger.severe(String.format("Memcache put failure for key=%s, encodedKey=%s\n", key, encodedKey));
        writer.fail();
        return;
      }
      writer.write(key.toString(), value);
    }
  }

  static String getEncodedKey(java.io.Serializable key)
    throws UnsupportedEncodingException, IOException {
      return BaseEncoding.base64().encode(MemcacheSerialization.makePbKey(key));
  }
}
