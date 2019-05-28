package de.dbathon.jds.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A {@link LinkedHashMap} from {@link String} to {@link Object} that has an
 * {@linkplain #add(String, Object) add method} that can be chained.
 */
public class JsonMap extends LinkedHashMap<String, Object> {

  public JsonMap() {}

  public JsonMap(final Map<? extends String, ? extends Object> map) {
    super(map);
  }

  public JsonMap add(final String key, final Object value) {
    put(key, value);
    return this;
  }

  public JsonMap addAll(final Map<String, ?> values) {
    putAll(values);
    return this;
  }

}
